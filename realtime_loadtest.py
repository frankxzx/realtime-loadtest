#!/usr/bin/env python3
"""
Azure OpenAI Realtime API 压测脚本 (GA)
测试 gpt-realtime-1.5 + gpt-realtime-whisper 的 RPM/TPM/并发上限

用法:
  cp .env.example .env && vi .env

  python3 realtime_loadtest.py --mode text --concurrency 10 --duration 60 --html
  python3 realtime_loadtest.py --mode audio --concurrency 5 --duration 60 --html
  python3 realtime_loadtest.py --mode text --ramp --ramp-start 1 --ramp-max 50 --ramp-step 5 --html

  # 转写(whisper)持续压测：--pipeline 管道化，总在途=并发×管道，只握手 10 次
  python3 realtime_loadtest.py --mode transcribe --transcribe-model gpt-realtime-whisper \\
      --language en --concurrency 10 --pipeline 10 --duration 120 --html

  # 转写脉冲测试：1000 个 commit 一次性发完，看什么量级报什么错(忽略 duration)
  python3 realtime_loadtest.py --mode transcribe --transcribe-model gpt-realtime-whisper \\
      --language en --burst 1000 --concurrency 10 --html

  # 保险坐席场景：mock 多轮历史(坐席一轮≈1分钟话术)，AI 扮演客户回话；
  # --sync-fire = 100 路先建连注好历史，同一时刻齐发 response.create(测同一时间戳并发)
  python3 realtime_loadtest.py --mode chat --concurrency 100 --sync-fire --html

  # 带期望配额对比（拿去跟 Azure 对峙用）
  python3 realtime_loadtest.py --mode text --concurrency 20 --duration 120 \\
      --expected-tpm 50000 --expected-rpm 100 --region eastus2 --html

chat 模式(保险坐席对话)关键参数:
  --sync-fire         全员建连+注入历史后集合，同一时刻齐发一轮 response.create，
                      打完即止(忽略 --duration)。握手仍按 --connect-stagger 错峰，
                      保证 429 是模型配额(session)而非握手限流(handshake)
  不加 --sync-fire    持续模式：每 worker 循环整段话本(每轮带全量历史上下文)到 duration

transcribe 模式关键参数:
  --reuse-conn        复用 WS：每 worker 一次握手，同连接循环转写(串行，每连接 1 在途)
  --pipeline N        管道化(隐含复用)：不等 completed 连发 N 个 commit，按 item_id 对账
  --burst N           脉冲：N 个请求均分到各连接一口气发完，等全部结算即止(与 --ramp 互斥)
  --connect-stagger   worker 首连错峰秒数(默认 0.25)，防同时握手撞 S0 onHandshake 429

429 归因: 报告区分 handshake(连接级,S0 tier 握手限流,非模型配额) / session(模型配额)。
whisper 按音频时长计费(usage.type=="duration"，无 token)：看 RPM 和转写速率(s/min)，TPM 恒 0。
"""

import asyncio
import websockets
from websockets.exceptions import InvalidStatus
import json
import time
import base64
import math
import struct
import os
import sys
import argparse
import statistics
import subprocess
import tempfile
import ssl
import traceback
import contextvars
from dataclasses import dataclass, field
from collections import deque
from datetime import datetime, timezone

# ─── SSL ────────────────────────────────────────────────────────────────────────
_SSL_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode = ssl.CERT_NONE


# ─── .env 加载 ──────────────────────────────────────────────────────────────────
def _load_dotenv() -> None:
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"),
        os.path.join(os.getcwd(), ".env"),
    ]
    for path in candidates:
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
            print(f"[env] loaded {path}")
            return
        except FileNotFoundError:
            continue


_load_dotenv()

# ─── 配置 ──────────────────────────────────────────────────────────────────────
ENDPOINT           = os.environ.get("AZURE_OPENAI_ENDPOINT", "").rstrip("/")
API_KEY            = os.environ.get("AZURE_OPENAI_API_KEY", "")
DEPLOYMENT         = os.environ.get("REALTIME_DEPLOYMENT", "gpt-realtime-1.5")
WHISPER_DEPLOYMENT = os.environ.get("WHISPER_DEPLOYMENT", "")

TEXT_PROMPTS = [
    "Reply with exactly: OK",
    "Say: yes",
    "One word: hello",
    "Answer: done",
    "Respond: ack",
]

# ─── chat 模式：保险坐席场景（mock 历史 + AI 扮演客户）──────────────────────────
# 坐席一轮 ≈ 1 分钟话术（中文口播约 270-300 字/分钟），客户简短回应。
# 历史经 conversation.item.create 注入：坐席=user(input_text)，客户=assistant(output_text)
# —— GA schema 已对照官方 openai SDK 类型定义核实（assistant 历史消息 content 用
# output_text，不是 beta 时代的 text）。
CHAT_CUSTOMER_INSTRUCTIONS = (
    "你在一通保险销售电话里扮演客户（消费者）本人，对方是保险公司坐席。"
    "请始终以客户身份用自然的中文口语回应：可以追问细节、讨价还价、表达犹豫或顾虑"
    "（预算、条款、理赔、家人意见等），不要轻易答应购买，也不要跳出角色、"
    "不要提及你是 AI。每次回复保持在两到四句话。"
)

CHAT_SCENARIOS = [
    {
        "name": "重疾险电销",
        "history": [
            ("agent",
             "哎您好，王先生，打扰您两分钟时间。我是安康人寿的客户经理小李，咱们上个月"
             "在您办车险的时候加过微信，当时您说对家庭保障这块有兴趣，让我这个月再跟您"
             "联系，所以今天特意给您回个电话。是这样的，我们公司这个月刚升级了一款终身"
             "重大疾病保险，叫安康福瑞版，保障范围从原来的一百种重疾扩展到了一百五十种，"
             "轻症中症都能多次赔付，而且轻症赔完之后重疾保额不减，这在同类产品里是很少"
             "见的。另外它还自带一个被保险人豁免，就是说万一在缴费期内不幸得了轻症，后"
             "面的保费就不用再交了，合同继续有效。您看您现在方便听我大概介绍一下吗，也"
             "就占用您几分钟时间。"),
            ("customer",
             "哦小李啊，我记得你。行，你简单说说吧，不过我先问一下，这种保险一年大概要"
             "交多少钱？我怕太贵了负担不起。"),
            ("agent",
             "好的王先生，价格这块您放心，我给您算一下。以您现在三十五岁的年龄，如果选"
             "五十万保额、三十年缴费期，平均下来一年大概是九千八左右，一个月也就八百多"
             "块钱，差不多就是一家人出去吃两顿饭的钱。而且我建议您选三十年缴，虽然总保"
             "费多一点，但是每年压力小，杠杆也高，万一出险，豁免的作用也更大。这五十万"
             "保额是确诊即赔的，不像医疗险要先垫钱再报销，拿到手的钱您想怎么用就怎么用，"
             "可以治病，也可以补贴家里这几年的收入损失。另外现在投保的话，我们这个月有"
             "一个健康服务权益，送三年的门诊绿通和专家二诊，这个平时自己买也要小几千块"
             "钱一年。"),
            ("customer",
             "九千八一年啊，说实话还是有点小贵。而且我有点搞不懂，我去年不是买过一个百"
             "万医疗险嘛，一年才几百块，能报三百万，那我为什么还要再花小一万买这个五十"
             "万的？"),
        ],
        "live_turns": [
            "王先生您这个问题问得特别好，这也是我最常被问到的。医疗险和重疾险其实是完"
            "全不同的两个东西，谁也替代不了谁。您那个百万医疗险，是报销型的，住院花了"
            "多少，凭发票报多少，钱是给医院的；但是您想过没有，真得了大病，最大的损失"
            "其实不是医药费，是收入。比如说不幸得个癌症，治疗加康复至少三五年不能正常"
            "上班，房贷、车贷、孩子的学费、老人的赡养费，这些医疗险一分钱都管不了。重"
            "疾险就是干这个的，确诊合同约定的疾病，五十万直接打到您卡上，相当于把您未"
            "来三五年的收入提前锁定了。所以标准的配置是医疗险管医药费，重疾险管收入损"
            "失，两个都得有，缺一个保障就是漏的。",
            "王先生，那要不这样，您也别急着今天就定，我把刚才说的这个方案做成一个详细"
            "的计划书，把保障责任、每年的保费、现金价值都列清楚，发到您微信上，您和嫂"
            "子晚上一起看一看。不过有一点我得提醒您，我们这个健康权益活动是到这个月三"
            "十号截止的，而且保费是跟着年龄走的，您过了这个生日再投保，每年就要多交三"
            "百多块，三十年下来也是一万多块钱的差别。要是您看完计划书觉得合适，咱们这"
            "周约个时间，我带上核保的同事上门给您做个双录，前后也就半个小时。您看是周"
            "六上午方便还是周日下午方便？",
        ],
    },
    {
        "name": "百万医疗险续保",
        "history": [
            ("agent",
             "您好，是陈女士吗？我是平惠保险的续保专员小张，工号八八零二。是这样的，您"
             "去年七月份在我们这里投保的那份百万医疗险，下个月十五号就到期了，系统提示"
             "您的续保通道已经开了，所以提前一个月给您打电话提醒一下。跟您同步一个好消"
             "息，您这一年没有出险记录，续保的时候可以享受无理赔优惠，保费在去年的基础"
             "上打九五折。另外今年这款产品升级了，外购药责任从一百种扩展到了一百五十种，"
             "CAR-T这种上百万的治疗手段也纳入报销了，质子重离子还是百分之百报。您看趁"
             "着这次续保，我帮您把这些新责任一起配置上好吗？"),
            ("customer",
             "哦对，是有这么个保险。到期了是吧？那个升级要加钱吗？加多少？"),
            ("agent",
             "陈女士，升级的费用其实很少。您去年的保费是六百八十六，今年九五折之后是六"
             "百五十二，把外购药和CAR-T这两个新责任加上，一共是七百一十九，也就是比去"
             "年多了三十几块钱。但是这三十几块钱买到的保障差别很大，现在肿瘤治疗最有效"
             "的靶向药、免疫治疗的药，一大半都是在医院外面的药房买的，没有外购药责任的"
             "话，这部分医疗险是不报的，自费下来一个月就是好几万。另外我看您的保单，您"
             "先生和孩子还没有加进来，咱们这款产品是可以家庭单投保的，一张保单保全家，"
             "三个人一起投的话整单还能再打九五折，您先生三十八岁，一年也就八百多。"),
            ("customer",
             "家庭单啊，那我得回去问问我们家那位。他这两年体检查出来有点脂肪肝还有结节，"
             "这种能买吗？会不会到时候不给报？"),
        ],
        "live_turns": [
            "陈女士，您问的这个正是关键。脂肪肝和结节要看具体情况，我们有智能核保，您"
            "不用提交任何纸质材料，在线上如实回答几个问题，马上就能出结论，是标准承保、"
            "除外承保还是加费承保，当场就知道，而且不会留任何拒保记录，对以后买别的保"
            "险也没有影响。像轻度脂肪肝、肝功能正常的，大部分是可以标准体承保的；结节"
            "要看是几级，一二级的甲状腺结节一般是除外甲状腺相关责任承保，其他的病还是"
            "照样保。我的建议是趁您先生现在这些还都是小问题，赶紧把保障定下来，真等到"
            "哪天体检结果再严重一点，想买都买不了了，医疗险的核保只会越来越严。",
            "那这样吧陈女士，我先把您本人的续保加升级办了，保证保障不断档，这个今天电"
            "话里就能确认，稍后我发短信给您，您点链接确认支付就可以。您先生和孩子的家"
            "庭单，我把智能核保的链接一起发到您微信，您让先生抽五分钟把健康问卷做一下，"
            "做完把结果截图给我，我帮您看看是什么承保结论，合适的话这个月内加进来，还"
            "能赶上整单九五折。您放心，核保通不过是不收一分钱的，也不留记录。那我现在"
            "就给您发短信了，您看这样安排可以吧？对了，您的微信还是尾号五六七八这个手"
            "机号吧？",
        ],
    },
    {
        "name": "增额寿/养老年金",
        "history": [
            ("agent",
             "喂，您好，李姐，我是您的保险服务人员小周啊，前两天在社区做活动的时候您在"
             "我们展台上登记过，说想了解一下养老这块的规划。今天给您打电话，是因为我们"
             "那款增额终身寿险月底就要停售了，想着一定得赶紧告诉您一声。这款产品简单说"
             "就是一个安全的长期储蓄账户，保额每年按百分之三点零复利递增，写进合同里，"
             "保证给付，不管外面利率怎么降、股市怎么跌，它都雷打不动地涨。您把闲钱放进"
             "去，第五年之后随时可以减保取现，当孩子的教育金，或者六十岁以后当养老金按"
             "月领，都很灵活。现在银行五年定期都降到一点五了，而且还在降，这种能锁定终"
             "身百分之三的产品，以后真的不会再有了。"),
            ("customer",
             "小周啊，我想起来了。复利三个点是吧，听着是比存银行强。不过我这钱放你们保"
             "险公司，万一你们公司倒闭了怎么办？我这养老钱可不敢有闪失。"),
            ("agent",
             "李姐，您这个担心特别正常，我给您讲清楚您就放心了。第一，保险公司是金融监"
             "管总局直接监管的，偿付能力每个季度都要公开披露，我们公司目前综合偿付能力"
             "充足率是百分之两百多，远超监管线。第二，保险法第九十二条写得明明白白，经"
             "营人寿保险业务的公司就算破产，它的保单也必须转让给其他保险公司，转让不出"
             "去的由监管指定接收，您的保单利益是法律兜底的。这么多年，咱们国家还没有一"
             "张寿险保单因为公司经营问题赔不了的。第三，您这个钱写进合同的现金价值，每"
             "一年值多少钱，合同上白纸黑字印着，不是那种看运气的分红。您要是还不放心，"
             "我可以把条款和现金价值表都打印出来给您看。"),
            ("customer",
             "行吧，那你说说，我要是一年放个两万，放十年，到我六十五岁能领多少？"),
        ],
        "live_turns": [
            "好嘞李姐，我拿您的情况给您算笔账啊。您今年四十八岁，每年交两万，交十年，"
            "一共投入二十万。到您六十岁的时候，账户的现金价值大概是二十四万八；到六十"
            "五岁，大概是二十八万七；如果一直放着不动，到八十岁能到四十四万多。领的方"
            "式很灵活，比如您六十五岁开始，每年从里面减保领两万，一直领到八十五岁，一"
            "共领了四十万，账户里还剩十来万可以留给孩子。而且这个账户领多少、什么时候"
            "领，完全您说了算，急用钱的时候还可以保单贷款，最高能贷现金价值的百分之八"
            "十，一个星期就到账，不影响合同继续增值。这笔钱进可攻退可守，您看这个数字"
            "您还满意吧？",
            "李姐，那我建议您这样，月底二十八号这款产品就正式停售了，现在系统里投保的"
            "人特别多，核保还需要一两天时间，咱们别卡到最后一天。您要是基本认可这个方"
            "案，我明天上午把计划书给您送过去，顺便帮您把投保资料先录进去，录进去不等"
            "于生效，还有十五天的犹豫期，这十五天里您随时反悔，一分钱不少地退给您。您"
            "就当先把这个百分之三的额度占上，免得月底想买买不到，那才是真的亏了。您明"
            "天上午十点左右在家吧？我到时候再带一份我们公司的偿付能力报告给您，您慢慢"
            "看。",
        ],
    },
]


# ─── 限流异常（携带完整错误信息）────────────────────────────────────────────────
class RateLimitError(Exception):
    def __init__(self, message: str, code: str = "", retry_after: str = ""):
        super().__init__(message)
        self.code = code
        self.retry_after = retry_after


# ─── 转写失败异常（非限流的 input_audio_transcription.failed）──────────────────
class TranscriptionFailed(Exception):
    def __init__(self, message: str, code: str = ""):
        super().__init__(message)
        self.code = code


# ─── 响应失败异常（response.done 里 status=failed/incomplete，非限流）──────────
class ResponseFailed(Exception):
    def __init__(self, message: str, code: str = "", status: str = ""):
        super().__init__(message)
        self.code = code
        self.status = status


# ─── 结构化日志 ─────────────────────────────────────────────────────────────────
_C = {
    "reset": "\033[0m", "gray": "\033[90m", "cyan": "\033[96m",
    "green": "\033[92m", "yellow": "\033[93m", "red": "\033[91m",
    "bold": "\033[1m", "dim": "\033[2m",
}
_T0 = time.monotonic()

# ─── 批次/序号上下文（asyncio 每个 task 独立，日志深处也能取到「第几批第几个」）──
_CTX_BATCH    = contextvars.ContextVar("batch",    default=0)   # 第几批(ramp 步)
_CTX_BATCH_CC = contextvars.ContextVar("batch_cc", default=0)   # 该批并发数
_CTX_SEQ      = contextvars.ContextVar("seq",      default=0)   # 批内第几个请求
_CTX_WORKER   = contextvars.ContextVar("worker",   default="")  # worker 标识


def _ctx() -> tuple[int, int, int, str]:
    return _CTX_BATCH.get(), _CTX_BATCH_CC.get(), _CTX_SEQ.get(), _CTX_WORKER.get()


class EventLog:
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.entries: list[dict] = []

    def _entry(self, level, worker, direction, event, detail="", error="") -> dict:
        batch, cc, seq, _ = _ctx()
        e = {
            "elapsed":   round(time.monotonic() - _T0, 3),
            "ts":        datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "batch":     batch,
            "batch_cc":  cc,
            "seq":       seq,
            "level":     level,
            "worker":    worker,
            "direction": direction,
            "event":     event,
            "detail":    detail,
            "error":     error,
        }
        self.entries.append(e)
        return e

    def _print(self, e: dict) -> None:
        lc = {"INFO": _C["green"], "WARN": _C["yellow"],
              "ERROR": _C["red"], "DEBUG": _C["dim"]}.get(e["level"], "")
        dc = {"→": _C["cyan"], "←": _C["green"], "✓": _C["green"] + _C["bold"],
              "✗": _C["red"], "⚡": _C["yellow"], "·": _C["gray"]}.get(e["direction"], "")
        ts  = f"{_C['gray']}{e['ts']}{_C['reset']}"
        bt  = f"{_C['cyan']}B{e['batch']}{_C['reset']}" if e["batch"] else "  "
        sq  = f"#{e['seq']}" if e["seq"] else ""
        wid = f"{_C['dim']}[{e['worker']:>3}{sq}]{_C['reset']}"
        d   = f"{dc}{e['direction']}{_C['reset']}"
        ev  = f"{lc}{e['event']:<28}{_C['reset']}"
        det = f"{_C['dim']}{e['detail']}{_C['reset']}" if e["detail"] else ""
        err = f" {_C['red']}{e['error']}{_C['reset']}" if e["error"] else ""
        print(f"{ts} {bt} {wid} {d} {ev}{det}{err}")

    def send(self, worker, event, payload=None):
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        self._print(self._entry("DEBUG", worker, "→", event, detail))

    def trigger(self, worker, event, detail=""):
        """请求触发点(如 commit 发出)：始终写入报告 entries(带时间戳可回溯)，
        控制台仅 verbose 时打印，避免高并发刷屏"""
        e = self._entry("INFO", worker, "→", event, detail)
        if self.verbose:
            self._print(e)

    def recv(self, worker, event, payload=None):
        if not self.verbose:
            return
        detail = _fmt_payload(payload) if payload else ""
        self._print(self._entry("DEBUG", worker, "←", event, detail))

    def info(self, worker, msg, detail=""):
        self._print(self._entry("INFO", worker, "·", msg, detail))

    def success(self, worker, event, detail=""):
        self._print(self._entry("INFO", worker, "✓", event, detail))

    def warn(self, worker, event, detail=""):
        self._print(self._entry("WARN", worker, "⚡", event, detail))

    def error(self, worker, event, detail="", exc: BaseException | None = None):
        err_str = ""
        if exc:
            tb = traceback.extract_tb(exc.__traceback__)
            if tb:
                last = tb[-1]
                err_str = f"{type(exc).__name__} @ {last.filename.split('/')[-1]}:{last.lineno}"
            else:
                err_str = f"{type(exc).__name__}: {exc}"
        self._print(self._entry("ERROR", worker, "✗", event, detail, err_str))

    def rate_limit(self, worker, detail=""):
        self._print(self._entry("WARN", worker, "⚡", "429 Rate Limited", detail))

    def write_html(self, path: str, meta: dict) -> None:
        payload = json.dumps({
            "rows": self.entries,
            "meta": meta,
        }, ensure_ascii=False)
        html = _HTML_TEMPLATE.replace("__PAYLOAD__", payload)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n[log] HTML 报告: {path}")


def _fmt_payload(p: dict) -> str:
    skip = {"audio", "instructions"}
    parts = []
    for k, v in p.items():
        if k in skip:
            continue
        sv = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
        parts.append(f"{k}={sv[:60]}")
    return "  " + "  ".join(parts) if parts else ""


LOG = EventLog(verbose=False)


# ─── 统计结构 ───────────────────────────────────────────────────────────────────
@dataclass
class GlobalStats:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    total_requests:      int   = 0
    success:             int   = 0
    failed:              int   = 0
    rate_limited_429:    int   = 0
    # 429 按来源拆分：handshake=WS 握手被拒(连接级，S0 tier 新建连接速率，跟转写模型无关)
    #                 session=会话内事件(transcription.failed/error，才是模型配额)
    rate_limited_handshake: int = 0
    rate_limited_session:   int = 0
    transcription_failed: int  = 0
    response_failed:     int   = 0
    timeouts:            int   = 0
    connection_errors:   int   = 0
    input_tokens:        int   = 0
    output_tokens:     int   = 0
    total_tokens:      int   = 0
    # whisper 家族按音频时长计费(usage.type=="duration"，无 token)，累计转写秒数
    transcribed_seconds: float = 0.0
    # usage 明细累计(官方 usage.input_token_details/output_token_details，
    # cached 是 input 的子集；键名对齐 GA schema，见 _extract_usage_details)
    usage_details: dict = field(default_factory=lambda: {
        "input_text_tokens": 0, "input_audio_tokens": 0, "input_image_tokens": 0,
        "input_cached_tokens": 0,
        "cached_text_tokens": 0, "cached_audio_tokens": 0, "cached_image_tokens": 0,
        "output_text_tokens": 0, "output_audio_tokens": 0,
    })

    req_timestamps:   deque = field(default_factory=deque)
    token_timestamps: deque = field(default_factory=deque)
    latencies:        list  = field(default_factory=list)
    errors:           list  = field(default_factory=list)

    # 新增：对峙用关键数据
    peak_rpm:           float        = 0.0
    peak_tpm:           float        = 0.0
    first_429_elapsed:  float | None = None
    first_429_rpm:      float | None = None
    first_429_tpm:      float | None = None
    rate_limit_details: list         = field(default_factory=list)
    timeseries:         list         = field(default_factory=list)  # [{elapsed,rpm,tpm,ok,e429}]

    # Azure 主动上报的配额 (rate_limits.updated 事件)：对峙金证据
    rate_limit_reports: list         = field(default_factory=list)  # [{elapsed,name,limit,remaining,reset_seconds}]
    declared_limits:    dict         = field(default_factory=dict)  # name -> {limit,min_remaining}

    # 批次/序号（第几批第几个）
    batch_index:        int          = 1
    batch_concurrency:  int          = 0
    seq:                int          = 0
    first_anomaly:      dict | None  = None   # 本批首次异常 {batch,batch_cc,seq,worker,elapsed,kind,detail}

    start_time: float = field(default_factory=time.monotonic)

    def next_seq(self) -> int:
        # asyncio 单线程、调用与使用间无 await，无需加锁
        self.seq += 1
        return self.seq

    def _mark_first_anomaly_unlocked(self, kind: str, detail: str, elapsed: float):
        if self.first_anomaly is not None:
            return
        b, cc, s, w = _ctx()
        self.first_anomaly = {
            "batch":    b, "batch_cc": cc, "seq": s, "worker": w,
            "elapsed":  round(elapsed, 1), "kind": kind, "detail": detail[:200],
        }
        print(f"\n{_C['red']}{_C['bold']}★★★ 首次异常 ★★★{_C['reset']} "
              f"{_C['yellow']}第 {b} 批(并发 {cc}) · 第 {s} 个请求 · {w}{_C['reset']} "
              f"[{kind}] @ +{elapsed:.1f}s  {detail[:80]}\n")

    async def record_success(self, input_tok: int, output_tok: int, latency: float,
                             audio_seconds: float = 0.0, usage_details: dict | None = None):
        async with self.lock:
            now = time.monotonic()
            self.total_requests += 1
            self.success += 1
            self.input_tokens  += input_tok
            self.output_tokens += output_tok
            self.total_tokens  += input_tok + output_tok
            self.transcribed_seconds += audio_seconds
            if usage_details:
                for k, v in usage_details.items():
                    if v and k in self.usage_details:
                        self.usage_details[k] += v
            self.latencies.append(latency)
            self.req_timestamps.append(now)
            for _ in range(input_tok + output_tok):
                self.token_timestamps.append(now)

    async def record_rate_limit(self, message: str, code: str = "", retry_after: str = "",
                                source: str = "session"):
        """source: "handshake"=WS 握手 HTTP 429（连接级，非模型配额）
                   "session"=会话内限流事件（真正命中模型/转写配额）"""
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.start_time
            self.total_requests += 1
            self.failed += 1
            self.rate_limited_429 += 1
            if source == "handshake":
                self.rate_limited_handshake += 1
            else:
                self.rate_limited_session += 1
            self.req_timestamps.append(now)

            rpm = self._rpm_unlocked()
            tpm = self._tpm_unlocked()
            b, cc, s, w = _ctx()

            if self.first_429_elapsed is None:
                self.first_429_elapsed = round(elapsed, 1)
                self.first_429_rpm     = rpm
                self.first_429_tpm     = tpm

            self.rate_limit_details.append({
                "elapsed":     round(elapsed, 1),
                "batch":       b,
                "batch_cc":    cc,
                "seq":         s,
                "worker":      w,
                "source":      source,
                "code":        code,
                "message":     message[:300],
                "retry_after": retry_after,
                "rpm":         rpm,
                "tpm":         tpm,
            })
            self.errors.append(f"[B{b}#{s} 429/{source}:{code}] {message[:100]}")
            self._mark_first_anomaly_unlocked(f"429_{source}", f"[{code}] {message}", elapsed)

    async def record_failure(self, reason: str):
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            b, _, s, _ = _ctx()
            self.total_requests += 1
            self.failed += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(f"[B{b}#{s}] {reason[:120]}")
            self._mark_first_anomaly_unlocked("failure", reason, elapsed)

    async def record_timeout(self, reason: str = "Timeout"):
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            b, _, s, _ = _ctx()
            self.total_requests += 1
            self.failed += 1
            self.timeouts += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(f"[B{b}#{s} TIMEOUT] {reason[:100]}")
            self._mark_first_anomaly_unlocked("timeout", reason, elapsed)

    async def record_transcription_failure(self, message: str, code: str = ""):
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            b, _, s, _ = _ctx()
            self.total_requests += 1
            self.failed += 1
            self.transcription_failed += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(f"[B{b}#{s} TRANSCRIBE_FAIL:{code}] {message[:100]}")
            self._mark_first_anomaly_unlocked("transcription_failed",
                                              f"[{code}] {message}", elapsed)

    async def record_response_failure(self, message: str, code: str = "", status: str = ""):
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            b, _, s, _ = _ctx()
            self.total_requests += 1
            self.failed += 1
            self.response_failed += 1
            self.req_timestamps.append(time.monotonic())
            self.errors.append(f"[B{b}#{s} RESP_{status}:{code}] {message[:100]}")
            self._mark_first_anomaly_unlocked("response_failed",
                                              f"[{status}/{code}] {message}", elapsed)

    async def record_rate_limits_updated(self, rate_limits: list):
        """记录 Azure 主动上报的配额，供报告展示"""
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            for rl in rate_limits:
                name      = rl.get("name", "")
                limit     = rl.get("limit")
                remaining = rl.get("remaining")
                self.rate_limit_reports.append({
                    "elapsed":       round(elapsed, 1),
                    "name":          name,
                    "limit":         limit,
                    "remaining":     remaining,
                    "reset_seconds": rl.get("reset_seconds"),
                })
                d = self.declared_limits.setdefault(name, {"limit": limit, "min_remaining": remaining})
                if limit is not None:
                    d["limit"] = limit
                if remaining is not None and (d["min_remaining"] is None or remaining < d["min_remaining"]):
                    d["min_remaining"] = remaining

    async def record_connection_error(self, reason: str):
        async with self.lock:
            elapsed = time.monotonic() - self.start_time
            b, _, s, _ = _ctx()
            self.connection_errors += 1
            self.errors.append(f"[B{b}#{s} CONN] {reason[:100]}")
            self._mark_first_anomaly_unlocked("conn_error", reason, elapsed)

    def _rpm_unlocked(self) -> float:
        now = time.monotonic()
        cutoff = now - 60
        return sum(1 for t in self.req_timestamps if t > cutoff)

    def _tpm_unlocked(self) -> float:
        cutoff = time.monotonic() - 60
        return sum(1 for t in self.token_timestamps if t > cutoff)

    def current_rpm(self) -> float:
        cutoff = time.monotonic() - 60
        while self.req_timestamps and self.req_timestamps[0] < cutoff:
            self.req_timestamps.popleft()
        return len(self.req_timestamps)

    def current_tpm(self) -> float:
        cutoff = time.monotonic() - 60
        while self.token_timestamps and self.token_timestamps[0] < cutoff:
            self.token_timestamps.popleft()
        return len(self.token_timestamps)

    def snapshot(self, elapsed: float):
        """monitor_loop 每 5s 调用一次，记录时序快照"""
        rpm = self.current_rpm()
        tpm = self.current_tpm()
        if rpm > self.peak_rpm:
            self.peak_rpm = rpm
        if tpm > self.peak_tpm:
            self.peak_tpm = tpm
        self.timeseries.append({
            "elapsed": round(elapsed, 1),
            "rpm":     round(rpm, 1),
            "tpm":     round(tpm, 1),
            "ok":      self.success,
            "e429":    self.rate_limited_429,
            "err":     self.failed,
        })

    def summary(self) -> dict:
        elapsed = time.monotonic() - self.start_time
        lats = self.latencies
        ok_rate = round(self.success / self.total_requests * 100, 1) if self.total_requests else 0
        return {
            "elapsed_s":          round(elapsed, 1),
            "total_requests":     self.total_requests,
            "success":            self.success,
            "success_rate_pct":   ok_rate,
            "failed":             self.failed,
            "rate_limited_429":   self.rate_limited_429,
            "rate_limited_handshake": self.rate_limited_handshake,
            "rate_limited_session":   self.rate_limited_session,
            "transcription_failed": self.transcription_failed,
            "response_failed":    self.response_failed,
            "timeouts":           self.timeouts,
            "connection_errors":  self.connection_errors,
            "declared_limits":    self.declared_limits,
            "input_tokens":       self.input_tokens,
            "output_tokens":      self.output_tokens,
            "total_tokens":       self.total_tokens,
            "usage_details":      dict(self.usage_details),
            "transcribed_audio_s": round(self.transcribed_seconds, 1),
            "avg_audio_s_per_min": round(self.transcribed_seconds / elapsed * 60, 1) if elapsed > 0 else 0,
            "avg_rpm":            round(self.success / elapsed * 60, 1) if elapsed > 0 else 0,
            "avg_tpm":            round(self.total_tokens / elapsed * 60, 1) if elapsed > 0 else 0,
            "peak_rpm":           round(self.peak_rpm, 1),
            "peak_tpm":           round(self.peak_tpm, 1),
            "first_429_elapsed_s": self.first_429_elapsed,
            "first_429_rpm":      self.first_429_rpm,
            "first_429_tpm":      self.first_429_tpm,
            "first_anomaly":      self.first_anomaly,
            "batch_index":        self.batch_index,
            "batch_concurrency":  self.batch_concurrency,
            "latency_p50_ms":     round(statistics.median(lats) * 1000, 1) if lats else 0,
            "latency_p95_ms":     round(sorted(lats)[int(len(lats) * 0.95)] * 1000, 1) if lats else 0,
            "latency_p99_ms":     round(sorted(lats)[int(len(lats) * 0.99)] * 1000, 1) if lats else 0,
            "latency_max_ms":     round(max(lats) * 1000, 1) if lats else 0,
        }


# ─── 测试音频 ───────────────────────────────────────────────────────────────────
# 固定使用仓库内自带的 hello_world.wav（真实 "Hello world" TTS，24kHz mono PCM16）。
# 这样每次跑都是同一段确定音频、转写结果稳定，且不依赖 say/ffmpeg。
_AUDIO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "hello_world.wav")


def _read_wav_pcm(path: str) -> bytes:
    """读 WAV，校验为 24kHz mono 16-bit，返回裸 PCM。"""
    import wave
    with wave.open(path, "rb") as w:
        ch, sw, sr, n = (w.getnchannels(), w.getsampwidth(),
                         w.getframerate(), w.getnframes())
        if (ch, sw, sr) != (1, 2, 24000):
            raise ValueError(f"WAV 需为 mono/16-bit/24kHz，实际 ch={ch} sw={sw} sr={sr}")
        return w.readframes(n)


def _generate_audio_via_say(text: str = "Hello world") -> bytes:
    """用 macOS say 直接输出 24kHz PCM16 WAV（无需 ffmpeg），返回裸 PCM。"""
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        wav_path = f.name
    try:
        subprocess.run(
            ["say", text, "--data-format=LEI16@24000", "--file-format=WAVE",
             "-o", wav_path],
            check=True, capture_output=True,
        )
        return _read_wav_pcm(wav_path)
    finally:
        try:
            os.unlink(wav_path)
        except FileNotFoundError:
            pass


def _generate_audio_fallback(duration_s: float = 1.5, sr: int = 24000) -> bytes:
    n = int(sr * duration_s)
    buf = bytearray()
    for i in range(n):
        buf += struct.pack("<h", int(math.sin(2 * math.pi * 440 * i / sr) * 0.6 * 32767))
    return bytes(buf)


def _load_test_audio() -> str:
    # 1) 优先用仓库内自带的固定 hello_world.wav
    if os.path.exists(_AUDIO_FILE):
        try:
            pcm = _read_wav_pcm(_AUDIO_FILE)
            print(f"[音频] 固定 hello_world.wav ({len(pcm) // 2 / 24000:.2f}s, 24kHz mono)")
            return base64.b64encode(pcm).decode()
        except Exception as e:
            print(f"[音频] hello_world.wav 读取失败({e})，尝试 say 生成")
    # 2) 本机 say 直出 PCM（无需 ffmpeg）
    try:
        pcm = _generate_audio_via_say("Hello world")
        print(f"[音频] macOS say 生成 'Hello world' ({len(pcm) // 2 / 24000:.2f}s)")
        return base64.b64encode(pcm).decode()
    except Exception as e:
        print(f"[音频] say 不可用({e})，回退到 440Hz 正弦波（转写无意义，仅测链路）")
        return base64.b64encode(_generate_audio_fallback()).decode()


TEST_AUDIO_B64 = _load_test_audio()


# ─── WebSocket ──────────────────────────────────────────────────────────────────
def build_ws_url(deployment: str) -> str:
    ep = ENDPOINT.replace("https://", "wss://").replace("http://", "ws://")
    return f"{ep}/openai/v1/realtime?model={deployment}"


async def _ws_send(ws, worker: str, payload: dict) -> None:
    LOG.send(worker, payload.get("type", "?"), payload)
    await ws.send(json.dumps(payload))


async def _wait_event(ws, worker: str, event_type: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"等待 {event_type} 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k != "type"})
        if t == event_type:
            return evt
        if t == "error":
            _raise_ws_error(evt)


async def _capture_rate_limits(stats, worker: str, evt: dict) -> None:
    """rate_limits.updated：Azure 主动上报的配额(limit/remaining/reset)，对峙金证据"""
    rls = evt.get("rate_limits", []) or []
    if stats is not None:
        await stats.record_rate_limits_updated(rls)
    brief = "  ".join(
        f"{r.get('name')}:{r.get('remaining')}/{r.get('limit')}(reset{r.get('reset_seconds')}s)"
        for r in rls)
    LOG.info(worker, "rate_limits.updated", brief)


def _handle_transcription_failed(worker: str, evt: dict, fatal: bool):
    """处理 input_audio_transcription.failed；限流抛 RateLimitError，其余按 fatal 决定"""
    err  = evt.get("error", {}) or {}
    code = err.get("code", "") or err.get("type", "")
    msg  = err.get("message", "") or json.dumps(evt)[:200]
    if _is_rate_limit(code, msg):
        LOG.rate_limit(worker, f"[transcription.failed {code}] {msg}")
        raise RateLimitError(msg, code=code)
    if fatal:
        LOG.error(worker, "transcription.failed", f"[{code}] {msg}")
        raise TranscriptionFailed(msg, code=code)
    LOG.warn(worker, "transcription.failed(非致命)", f"[{code}] {msg}")


async def _wait_response_done(ws, worker: str, timeout: float, stats=None) -> tuple[int, int, dict]:
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("等待 response.done 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k not in ("type", "delta")})
        if t == "response.done":
            resp   = evt.get("response", {}) or {}
            status = resp.get("status", "completed")
            usage  = resp.get("usage", {}) or {}
            if status == "failed":
                # response 级失败：可能是限流/服务端错误/内容过滤
                sd   = resp.get("status_details", {}) or {}
                err  = sd.get("error", {}) or {}
                code = err.get("code", "") or sd.get("reason", "") or status
                msg  = err.get("message", "") or json.dumps(sd)[:200]
                if _is_rate_limit(code, msg):
                    LOG.rate_limit(worker, f"[response.failed {code}] {msg}")
                    raise RateLimitError(msg, code=code)
                LOG.error(worker, "response.failed", f"[{code}] {msg}")
                raise ResponseFailed(msg, code=code, status=status)
            if status in ("incomplete", "cancelled"):
                # 非致命：hit max_tokens/被打断，仍拿到部分 token，计成功但告警
                sd = resp.get("status_details", {}) or {}
                LOG.warn(worker, f"response.{status}", json.dumps(sd)[:120])
            return (usage.get("input_tokens", 0), usage.get("output_tokens", 0),
                    _extract_usage_details(usage))
        if t == "rate_limits.updated":
            await _capture_rate_limits(stats, worker, evt)
        if t == "conversation.item.input_audio_transcription.failed":
            _handle_transcription_failed(worker, evt, fatal=False)  # 音频对话里非致命
        if t == "error":
            _raise_ws_error(evt)


_USAGE_MISSING_WARNED = False   # usage 真缺失只警告一次，别刷屏


def _extract_usage_details(usage: dict) -> dict:
    """usage.input_token_details/output_token_details → 扁平明细，键名对齐 GA schema。
    cached 是 input 的子集(input_tokens 已含 cached)；转写 tokens 形态只有
    input 的 text/audio；whisper duration 形态无 token，全 0。"""
    ind  = usage.get("input_token_details", {}) or {}
    outd = usage.get("output_token_details", {}) or {}
    cad  = ind.get("cached_tokens_details", {}) or {}
    return {
        "input_text_tokens":   ind.get("text_tokens", 0) or 0,
        "input_audio_tokens":  ind.get("audio_tokens", 0) or 0,
        "input_image_tokens":  ind.get("image_tokens", 0) or 0,
        "input_cached_tokens": ind.get("cached_tokens", 0) or 0,
        "cached_text_tokens":  cad.get("text_tokens", 0) or 0,
        "cached_audio_tokens": cad.get("audio_tokens", 0) or 0,
        "cached_image_tokens": cad.get("image_tokens", 0) or 0,
        "output_text_tokens":  outd.get("text_tokens", 0) or 0,
        "output_audio_tokens": outd.get("audio_tokens", 0) or 0,
    }


def _usage_brief(in_tok: int, out_tok: int, d: dict) -> str:
    """日志用 usage 摘要：in=25(text12+audio13,cached4) out=30(text30)，只列非零项"""
    def _parts(*pairs) -> str:
        return "+".join(f"{name}{n}" for name, n in pairs if n)
    inp = _parts(("text", d["input_text_tokens"]), ("audio", d["input_audio_tokens"]),
                 ("image", d["input_image_tokens"]))
    if d["input_cached_tokens"]:
        inp = f"{inp},cached{d['input_cached_tokens']}" if inp else f"cached{d['input_cached_tokens']}"
    outp = _parts(("text", d["output_text_tokens"]), ("audio", d["output_audio_tokens"]))
    return (f"in={in_tok}" + (f"({inp})" if inp else "")
            + f" out={out_tok}" + (f"({outp})" if outp else ""))


def _parse_transcription_usage(worker: str, evt: dict) -> tuple[int, int, float, str, dict]:
    """从 transcription.completed 事件解析 (in_tok, out_tok, audio_seconds, transcript, usage明细)。

    usage 有两种官方形态：
    - tokens:   {input_tokens, output_tokens, input_token_details...}（gpt-4o-transcribe 系）
    - duration: {"type":"duration","seconds":N}（whisper 家族按音频时长计费，无 token，
      gpt-realtime-whisper 即此类——不是异常，别告警）
    """
    global _USAGE_MISSING_WARNED
    usage      = evt.get("usage", {}) or {}
    transcript = evt.get("transcript", "")
    # whisper 家族：按时长计费，正常形态
    if usage.get("type") == "duration":
        return 0, 0, float(usage.get("seconds") or 0.0), transcript, _extract_usage_details({})
    details = usage.get("input_token_details", {}) or {}
    in_tok  = usage.get("input_tokens", 0)
    out_tok = usage.get("output_tokens", 0)
    # 兜底1: 顶层 input_tokens 缺失时，用 audio+text 明细求和
    if not in_tok and details:
        in_tok = details.get("audio_tokens", 0) + details.get("text_tokens", 0)
    # 兜底2: 仍拿到 total 但拆不出，用 total 当 in
    if not in_tok and usage.get("total_tokens"):
        in_tok = usage["total_tokens"] - out_tok
    # 诊断: tokens/duration 两种形态都没有才算真缺失，只报一次
    if (not usage or (in_tok == 0 and out_tok == 0)) and not _USAGE_MISSING_WARNED:
        _USAGE_MISSING_WARNED = True
        LOG.warn(worker, "transcription usage 缺失(仅提示一次)",
                 json.dumps({k: v for k, v in evt.items() if k != "logprobs"})[:300])
    return in_tok, out_tok, 0.0, transcript, _extract_usage_details(usage)


async def _wait_transcription_completed(ws, worker: str, timeout: float,
                                        stats=None) -> tuple[int, int, float, str, dict]:
    """等待 conversation.item.input_audio_transcription.completed，
    返回 (in_tok, out_tok, audio_seconds, transcript, usage明细)"""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise asyncio.TimeoutError("等待 transcription.completed 超时")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        evt = json.loads(raw)
        t = evt.get("type", "")
        LOG.recv(worker, t, {k: v for k, v in evt.items() if k not in ("type", "delta")})
        if t == "rate_limits.updated":
            await _capture_rate_limits(stats, worker, evt)
            continue
        if t == "conversation.item.input_audio_transcription.completed":
            return _parse_transcription_usage(worker, evt)
        if t == "conversation.item.input_audio_transcription.failed":
            _handle_transcription_failed(worker, evt, fatal=True)  # 转写模式里是致命
        if t == "error":
            _raise_ws_error(evt)


def _is_rate_limit(code, msg: str) -> bool:
    c = str(code).lower()
    m = (msg or "").lower()
    return ("rate" in m or "429" in str(code) or "rate_limit" in c
            or "too many requests" in m or "quota" in m or "exceeded" in m)


def _raise_ws_error(evt: dict):
    err  = evt.get("error", {})
    code = err.get("code", "")
    msg  = err.get("message", str(evt))
    if _is_rate_limit(code, msg):
        raise RateLimitError(msg, code=code)
    raise Exception(f"[{code}] {msg}")


def _parse_invalid_status(e: InvalidStatus) -> tuple[bool, str, str, str]:
    """返回 (is_429, code, message, retry_after)"""
    is_429 = e.response.status_code == 429
    code = retry_after = message = ""
    try:
        body = json.loads(e.response.body)
        code    = body.get("error", {}).get("code", "")
        message = body.get("error", {}).get("message", "")
    except Exception:
        message = str(e)
    try:
        retry_after = dict(e.response.headers).get("Retry-After", "")
    except Exception:
        pass
    return is_429, code, message or str(e), retry_after


# ─── 单次会话：文本 ─────────────────────────────────────────────────────────────
async def run_text_session(
    stats: GlobalStats, deployment: str, worker_id: int,
    prompt_idx: int, timeout: float = 30.0,
) -> None:
    wid    = f"W{worker_id:02d}"
    prompt = TEXT_PROMPTS[prompt_idx % len(TEXT_PROMPTS)]
    t_start = time.monotonic()
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "instructions": "You are a minimal assistant. Reply as briefly as possible.",
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            await _ws_send(ws, wid, {
                "type": "conversation.item.create",
                "item": {
                    "type": "message", "role": "user",
                    "content": [{"type": "input_text", "text": prompt}],
                },
            })
            await _ws_send(ws, wid, {"type": "response.create"})
            input_tok, output_tok, usage_d = await _wait_response_done(ws, wid, timeout=timeout, stats=stats)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"{_usage_brief(input_tok, output_tok, usage_d)} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency, usage_details=usage_d)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after, source="session")
    except ResponseFailed as e:
        LOG.error(wid, f"response.{e.status}", f"[{e.code}] {e}")
        await stats.record_response_failure(str(e), e.code, e.status)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[握手429/连接级] [{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after, source="handshake")
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_timeout(str(e) or "Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── 同步开火（--sync-fire）────────────────────────────────────────────────────
class SyncFire:
    """所有 worker 建连+注入完历史后集合，同一时刻齐发 response.create。

    worker 就绪调 report_ready()；没走到集合点就挂了（握手失败等）调 abandon()，
    避免全场干等。编排侧等 all_ready（带超时兜底）后 set fire 开火。
    """
    def __init__(self, total: int):
        self.total = total
        self.ready = 0
        self.abandoned = 0
        self.fire = asyncio.Event()       # 开火信号
        self.all_ready = asyncio.Event()  # 全员到齐（含中途放弃的）
        self.fired_at_utc = ""

    def report_ready(self) -> None:
        self.ready += 1
        self._check()

    def abandon(self) -> None:
        self.abandoned += 1
        self._check()

    def _check(self) -> None:
        if self.ready + self.abandoned >= self.total:
            self.all_ready.set()


# ─── 单次会话：保险坐席多轮对话（mock 历史 + AI 扮演客户）───────────────────────
async def run_chat_session(
    stats: GlobalStats, deployment: str, worker_id: int,
    timeout: float = 60.0, stop_event: asyncio.Event | None = None,
    sync: SyncFire | None = None,
) -> None:
    """
    保险坐席场景：注入 mock 聊天历史（坐席一轮 ≈ 1 分钟话术），模型扮演客户，
    对坐席的最新话术生成回复。每个 live turn = 1 个请求（response.create）。
    - 历史注入：坐席=user/input_text，客户=assistant/output_text（GA schema）
    - sync 非空（--sync-fire）：注好历史后到集合点等开火信号，全场同一时刻
      发出 response.create，只打这一轮（齐射），延迟从开火时刻起算
    - sync 为空：live turns 逐轮打完（每轮都是完整的历史+新话术上下文），
      会话结束后由 worker_loop 重连开新会话，直到 duration 到点
    """
    wid = f"W{worker_id:02d}"
    scenario = CHAT_SCENARIOS[worker_id % len(CHAT_SCENARIOS)]
    reported = False
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected", f"场景: {scenario['name']}")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "instructions": CHAT_CUSTOMER_INSTRUCTIONS,
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            # 注入 mock 历史（WS 保序，服务端按序建 item，无需逐条等 created）
            for role, text in scenario["history"]:
                if role == "agent":
                    item = {"type": "message", "role": "user",
                            "content": [{"type": "input_text", "text": text}]}
                else:
                    item = {"type": "message", "role": "assistant",
                            "content": [{"type": "output_text", "text": text}]}
                await _ws_send(ws, wid, {"type": "conversation.item.create", "item": item})

            for i, turn in enumerate(scenario["live_turns"]):
                _CTX_SEQ.set(stats.next_seq())
                await _ws_send(ws, wid, {
                    "type": "conversation.item.create",
                    "item": {"type": "message", "role": "user",
                             "content": [{"type": "input_text", "text": turn}]},
                })
                if i == 0 and sync is not None:
                    LOG.info(wid, "ready", "历史已注入，等待同步开火")
                    reported = True
                    sync.report_ready()
                    await sync.fire.wait()
                t_turn = time.monotonic()
                LOG.trigger(wid, "response.create", f"轮{i + 1}")
                await _ws_send(ws, wid, {"type": "response.create"})
                input_tok, output_tok = await _wait_response_done(
                    ws, wid, timeout=timeout, stats=stats)
                latency = time.monotonic() - t_turn
                LOG.success(wid, "response.done",
                            f"轮{i + 1} in={input_tok} out={output_tok} lat={latency:.2f}s")
                await stats.record_success(input_tok, output_tok, latency)
                if sync is not None:
                    break   # 齐射模式只打同步的这一轮
                if stop_event is not None and stop_event.is_set():
                    break

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after, source="session")
    except ResponseFailed as e:
        LOG.error(wid, f"response.{e.status}", f"[{e.code}] {e}")
        await stats.record_response_failure(str(e), e.code, e.status)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[握手429/连接级] [{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after, source="handshake")
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_timeout(str(e) or "Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))
    finally:
        if sync is not None and not reported:
            sync.abandon()   # 没到集合点就挂了，别让全场干等


# ─── 单次会话：音频 ─────────────────────────────────────────────────────────────
async def run_audio_session(
    stats: GlobalStats, deployment: str, worker_id: int, timeout: float = 45.0,
) -> None:
    wid = f"W{worker_id:02d}"
    t_start = time.monotonic()
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "instructions": "Transcribe the audio and reply with one word.",
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "noise_reduction": {"type": "far_field"},
                            "transcription": {
                                "model": WHISPER_DEPLOYMENT or "gpt-realtime-whisper",
                            },
                            "turn_detection": {
                                "type": "server_vad",
                                "threshold": 0.7,
                                "prefix_padding_ms": 200,
                                "silence_duration_ms": 800,
                                "create_response": False,
                            },
                        },
                        "output": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "voice": "alloy",
                        },
                    },
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            await _ws_send(ws, wid, {"type": "input_audio_buffer.append", "audio": TEST_AUDIO_B64})
            await _ws_send(ws, wid, {"type": "response.create"})
            input_tok, output_tok, usage_d = await _wait_response_done(ws, wid, timeout=timeout, stats=stats)
            latency = time.monotonic() - t_start
            LOG.success(wid, "response.done",
                        f"{_usage_brief(input_tok, output_tok, usage_d)} lat={latency:.2f}s")
            await stats.record_success(input_tok, output_tok, latency, usage_details=usage_d)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after, source="session")
    except ResponseFailed as e:
        LOG.error(wid, f"response.{e.status}", f"[{e.code}] {e}")
        await stats.record_response_failure(str(e), e.code, e.status)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[握手429/连接级] [{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after, source="handshake")
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_timeout(str(e) or "Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── 单次会话：转写专用（input audio transcription 模型独立压测）─────────────────
async def run_transcribe_session(
    stats: GlobalStats, deployment: str, worker_id: int,
    transcribe_model: str, language: str = "", timeout: float = 45.0,
    reuse: bool = False, stop_event: asyncio.Event | None = None,
    request_interval: float = 0.0,
) -> None:
    """
    纯转写会话：session.type="realtime" + output_modalities=["text"]，靠不发
    response.create 来避免任何 LLM 补全，只命中 input audio transcription
    模型（gpt-realtime-whisper），从而干净地测量转写模型自己的 RPM/TPM。
    连接走 realtime 部署，转写模型部署名放在 audio.input.transcription.model。
    turn_detection=None，手动 commit 触发转写。
    (output_modalities 不能为空数组，API 要求至少含 text 或 audio)

    reuse=True：同一条 WS 上循环 append→commit→completed，直到 stop_event。
    每次 commit 都是独立的转写请求，但 realtime 会话只建一次——不这样做的话
    每个转写都要新建 realtime 会话，realtime 部署的连接级 429 会先于 whisper
    配额触发，测不到转写模型自己的上限。
    """
    wid = f"W{worker_id:02d}"
    t_start = time.monotonic()
    transcription: dict = {"model": transcribe_model}
    if language:
        transcription["language"] = language
    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "noise_reduction": {"type": "far_field"},
                            "transcription": transcription,
                            "turn_detection": None,
                        },
                    },
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)
            n_done = 0
            while True:
                if n_done:  # 复用连接的后续请求：换新 seq，重新计时
                    _CTX_SEQ.set(stats.next_seq())
                t_req = t_start if n_done == 0 else time.monotonic()
                try:
                    await _ws_send(ws, wid, {"type": "input_audio_buffer.append",
                                             "audio": TEST_AUDIO_B64})
                    await _ws_send(ws, wid, {"type": "input_audio_buffer.commit"})
                    LOG.trigger(wid, "commit(trigger)")
                    input_tok, output_tok, audio_s, transcript, usage_d = await _wait_transcription_completed(
                        ws, wid, timeout=timeout, stats=stats)
                    latency = time.monotonic() - t_req
                    usage_s = (f"dur={audio_s:.2f}s" if audio_s
                               else _usage_brief(input_tok, output_tok, usage_d))
                    LOG.success(wid, "transcription.completed",
                                f'{usage_s} sent@+{t_req - _T0:.1f}s lat={latency:.2f}s "{transcript[:30]}"')
                    await stats.record_success(input_tok, output_tok, latency,
                                               audio_seconds=audio_s, usage_details=usage_d)
                except RateLimitError as e:
                    # 转写级限流（transcription.failed / error 事件）：会话还活着，
                    # 复用模式下记录后继续压——这正是我们要测的 whisper 429
                    LOG.rate_limit(wid, f"[{e.code}] {e}")
                    await stats.record_rate_limit(str(e), e.code, e.retry_after, source="session")
                    if not reuse:
                        return
                except TranscriptionFailed as e:
                    LOG.error(wid, "transcription.failed", f"[{e.code}] {e}")
                    await stats.record_transcription_failure(str(e), e.code)
                    if not reuse:
                        return
                n_done += 1
                if not reuse or (stop_event is not None and stop_event.is_set()):
                    return
                if request_interval > 0:
                    await asyncio.sleep(request_interval)

    except RateLimitError as e:
        LOG.rate_limit(wid, f"[{e.code}] {e}")
        await stats.record_rate_limit(str(e), e.code, e.retry_after, source="session")
    except TranscriptionFailed as e:
        LOG.error(wid, "transcription.failed", f"[{e.code}] {e}")
        await stats.record_transcription_failure(str(e), e.code)
    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[握手429/连接级] [{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after, source="handshake")
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError as e:
        LOG.error(wid, "Timeout", str(e), e)
        await stats.record_timeout(str(e) or "Timeout")
    except Exception as e:
        LOG.error(wid, "Exception", str(e), e)
        await stats.record_connection_error(str(e))


# ─── 单次会话：转写管道化（一条连接多个在途 commit，打满转写模型）───────────────
async def run_transcribe_pipelined(
    stats: GlobalStats, deployment: str, worker_id: int,
    transcribe_model: str, language: str = "", timeout: float = 45.0,
    pipeline: int = 8, stop_event: asyncio.Event | None = None,
    max_requests: int = 0,
) -> None:
    """
    管道化转写：不等上一个转写完成就连续 append→commit，保持每条连接
    `pipeline` 个在途转写。commit 是异步的——每次 commit 生成独立 conversation
    item 并触发独立转写；官方文档明确"完成事件顺序不保证，用 item_id 匹配"，
    即服务端并行处理多个在途转写。
    总在途 = concurrency × pipeline，这才是打满转写模型配额的压力形态；
    串行模式(run_transcribe_session)每连接同时只有 1 个在途，吞吐被单次
    转写延迟(~4.5s)限死。
    配对：input_audio_buffer.committed 的 item_id 按 FIFO 对应 commit 顺序，
    completed/failed 按 item_id 结算 latency 与 seq 归属。
    max_requests>0 = burst 模式：本连接总共只发这么多个 commit，发完 drain
    （等全部在途结算）后返回，不再补发。
    """
    wid = f"W{worker_id:02d}"
    transcription: dict = {"model": transcribe_model}
    if language:
        transcription["language"] = language
    sent_queue: deque = deque()   # 已 commit 待配 item_id: (seq, t_sent)
    inflight:   dict  = {}        # item_id -> (seq, t_sent)

    def _settle(item_id: str):
        """按 item_id 找回 (seq, t_sent)；配不上则 FIFO 兜底"""
        if item_id in inflight:
            return inflight.pop(item_id)
        if sent_queue:
            return sent_queue.popleft()
        return (_CTX_SEQ.get(), time.monotonic())

    try:
        async with websockets.connect(
            build_ws_url(deployment),
            additional_headers={"api-key": API_KEY},
            open_timeout=10, close_timeout=5, ssl=_SSL_CTX,
        ) as ws:
            LOG.info(wid, "connected", f"pipeline={pipeline}")
            await _wait_event(ws, wid, "session.created", timeout=10)
            await _ws_send(ws, wid, {
                "type": "session.update",
                "session": {
                    "type": "realtime",
                    "output_modalities": ["text"],
                    "audio": {
                        "input": {
                            "format": {"type": "audio/pcm", "rate": 24000},
                            "noise_reduction": {"type": "far_field"},
                            "transcription": transcription,
                            "turn_detection": None,
                        },
                    },
                },
            })
            await _wait_event(ws, wid, "session.updated", timeout=10)

            sent_total = 0
            while True:
                stopping = ((stop_event is not None and stop_event.is_set())
                            or (max_requests > 0 and sent_total >= max_requests))
                # 填满管道（stop/发满配额后不再发新的，只等在途的结算）
                while (not stopping and len(sent_queue) + len(inflight) < pipeline
                       and (max_requests == 0 or sent_total < max_requests)):
                    seq = stats.next_seq()
                    _CTX_SEQ.set(seq)
                    await _ws_send(ws, wid, {"type": "input_audio_buffer.append",
                                             "audio": TEST_AUDIO_B64})
                    await _ws_send(ws, wid, {"type": "input_audio_buffer.commit"})
                    sent_queue.append((seq, time.monotonic()))
                    sent_total += 1
                    LOG.trigger(wid, "commit(trigger)",
                                f"inflight={len(sent_queue) + len(inflight)}")
                stopping = ((stop_event is not None and stop_event.is_set())
                            or (max_requests > 0 and sent_total >= max_requests))
                if stopping and not sent_queue and not inflight:
                    return

                raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                evt = json.loads(raw)
                t = evt.get("type", "")
                LOG.recv(wid, t, {k: v for k, v in evt.items() if k not in ("type", "delta")})

                if t == "input_audio_buffer.committed":
                    # committed 顺序 == commit 发送顺序，FIFO 配 item_id
                    item_id = evt.get("item_id", "")
                    if item_id and sent_queue:
                        inflight[item_id] = sent_queue.popleft()
                elif t == "conversation.item.input_audio_transcription.completed":
                    seq, t_sent = _settle(evt.get("item_id", ""))
                    _CTX_SEQ.set(seq)
                    latency = time.monotonic() - t_sent
                    in_tok, out_tok, audio_s, transcript, usage_d = _parse_transcription_usage(wid, evt)
                    usage_s = f"dur={audio_s:.2f}s" if audio_s else _usage_brief(in_tok, out_tok, usage_d)
                    LOG.success(wid, "transcription.completed",
                                f'{usage_s} sent@+{t_sent - _T0:.1f}s lat={latency:.2f}s '
                                f'inflight={len(inflight)+len(sent_queue)} "{transcript[:30]}"')
                    await stats.record_success(in_tok, out_tok, latency,
                                               audio_seconds=audio_s, usage_details=usage_d)
                elif t == "conversation.item.input_audio_transcription.failed":
                    seq, _ = _settle((evt.get("item_id") or ""))
                    _CTX_SEQ.set(seq)
                    try:
                        _handle_transcription_failed(wid, evt, fatal=True)
                    except RateLimitError as e:   # 转写限流：模型配额 429，记录后继续压
                        await stats.record_rate_limit(str(e), e.code, e.retry_after,
                                                      source="session")
                    except TranscriptionFailed as e:
                        await stats.record_transcription_failure(str(e), e.code)
                elif t == "rate_limits.updated":
                    await _capture_rate_limits(stats, wid, evt)
                elif t == "error":
                    try:
                        _raise_ws_error(evt)
                    except RateLimitError as e:   # 会话级限流：记录后继续（连接还活着）
                        LOG.rate_limit(wid, f"[{e.code}] {e}")
                        await stats.record_rate_limit(str(e), e.code, e.retry_after,
                                                      source="session")

    except InvalidStatus as e:
        is_429, code, msg, retry_after = _parse_invalid_status(e)
        if is_429:
            LOG.rate_limit(wid, f"[握手429/连接级] [{code}] {msg} retry_after={retry_after}")
            await stats.record_rate_limit(msg, code, retry_after, source="handshake")
        else:
            LOG.error(wid, f"HTTP {e.response.status_code}", msg, e)
            await stats.record_failure(msg)
    except asyncio.TimeoutError:
        # timeout 秒内没有任何事件：把所有在途按超时结算，断连重连
        n_lost = len(sent_queue) + len(inflight)
        LOG.error(wid, "Timeout", f"{timeout}s 无事件，{n_lost} 个在途转写按超时计")
        for _ in range(max(1, n_lost)):
            await stats.record_timeout(f"pipeline {timeout}s 无事件")
    except Exception as e:
        # 连接中断（如服务端 1007 断连）：在途请求已发出未结算，必须按失败计，
        # 否则 burst 总数对不上、成功率虚高
        n_lost = len(sent_queue) + len(inflight)
        LOG.error(wid, "Exception", f"{e}  ({n_lost} 个在途按连接错误计)", e)
        await stats.record_connection_error(str(e))
        for _ in range(n_lost):
            await stats.record_failure(f"连接中断，在途转写丢失: {str(e)[:80]}")


# ─── Worker 池 ──────────────────────────────────────────────────────────────────
async def worker_loop(
    stats: GlobalStats, mode: str, deployment: str,
    stop_event: asyncio.Event, worker_id: int, request_interval: float = 0.0,
    transcribe_model: str = "", language: str = "",
    reuse_conn: bool = False, connect_stagger: float = 0.0,
    pipeline: int = 1, burst_share: int = 0,
    sync: SyncFire | None = None,
) -> None:
    # 每个 worker 是独立 task，contextvars 互不干扰
    _CTX_BATCH.set(stats.batch_index)
    _CTX_BATCH_CC.set(stats.batch_concurrency)
    _CTX_WORKER.set(f"W{worker_id:02d}")
    # 启动错峰：避免一批 worker 同时握手撞上 S0 tier 的连接建立速率限制
    # (onHandshake 429，连接级)，把握手 429 和模型配额 429 混在一起没法归因
    if connect_stagger > 0 and worker_id > 0:
        await asyncio.sleep(worker_id * connect_stagger)
    # burst 模式：一口气把份额全部 commit(管道深度=份额)，等全部结算后结束，
    # 不重连不补发——就是要看"瞬间打 N 个请求"服务端会怎么报错
    if mode == "transcribe" and burst_share > 0:
        await run_transcribe_pipelined(stats, deployment, worker_id,
                                       transcribe_model, language,
                                       pipeline=burst_share, stop_event=stop_event,
                                       max_requests=burst_share)
        return
    idx = worker_id
    while not stop_event.is_set():
        if mode == "chat":
            # 会话内部按 live turn 自行取 seq（一轮=一个请求）
            await run_chat_session(stats, deployment, worker_id,
                                   stop_event=stop_event, sync=sync)
            if sync is not None:
                return   # --sync-fire：齐射一轮即止，不重连不补发
            idx += 1
            if request_interval > 0:
                await asyncio.sleep(request_interval)
            continue
        _CTX_SEQ.set(stats.next_seq())   # 批内第几个请求（全 worker 共享递增）
        if mode == "text":
            await run_text_session(stats, deployment, worker_id, idx)
        elif mode == "transcribe":
            # 会话内部自循环直到 stop_event；连接挂了才回到这里重连
            if pipeline > 1:
                await run_transcribe_pipelined(stats, deployment, worker_id,
                                               transcribe_model, language,
                                               pipeline=pipeline, stop_event=stop_event)
            else:
                await run_transcribe_session(stats, deployment, worker_id,
                                             transcribe_model, language,
                                             reuse=reuse_conn, stop_event=stop_event,
                                             request_interval=request_interval)
        else:
            await run_audio_session(stats, deployment, worker_id)
        idx += 1
        if request_interval > 0:
            await asyncio.sleep(request_interval)


# ─── 实时监控 ───────────────────────────────────────────────────────────────────
async def monitor_loop(
    stats: GlobalStats, stop_event: asyncio.Event, interval: float = 5.0,
) -> None:
    hdr = (f"\n{'─'*80}\n"
           f"{'时间':>6}  {'RPM(1m)':>8}  {'TPM(1m)':>9}  {'成功':>6}  "
           f"{'429':>5}  {'失败':>5}  {'P50ms':>7}  {'P95ms':>7}  {'总Token':>9}\n"
           f"{'─'*80}")
    print(hdr)
    t0 = time.monotonic()
    while not stop_event.is_set():
        await asyncio.sleep(interval)
        elapsed = time.monotonic() - t0
        stats.snapshot(elapsed)
        s    = stats.summary()
        snap = stats.timeseries[-1] if stats.timeseries else {"rpm": 0.0, "tpm": 0.0}
        c429 = _C["yellow"] if s["rate_limited_429"] > 0 else ""
        rst  = _C["reset"]  if s["rate_limited_429"] > 0 else ""
        print(
            f"{int(elapsed):>5}s"
            f"  {snap['rpm']:>8.0f}"
            f"  {snap['tpm']:>9.0f}"
            f"  {s['success']:>6}"
            f"  {c429}{s['rate_limited_429']:>5}{rst}"
            f"  {s['failed']:>5}"
            f"  {s['latency_p50_ms']:>7.0f}"
            f"  {s['latency_p95_ms']:>7.0f}"
            f"  {s['total_tokens']:>9}"
        )


# ─── 主压测 ─────────────────────────────────────────────────────────────────────
async def run_load_test(
    mode: str, deployment: str, concurrency: int, duration: float,
    request_interval: float = 0.0, transcribe_model: str = "", language: str = "",
    batch_index: int = 1, reuse_conn: bool = False, connect_stagger: float = 0.0,
    pipeline: int = 1, burst: int = 0, sync_fire: bool = False,
) -> GlobalStats:
    print(f"\n{_C['bold']}[压测]{_C['reset']} "
          f"第{batch_index}批 mode={mode} deployment={deployment} "
          f"concurrency={concurrency} "
          f"{'sync-fire(齐射一轮即止)' if sync_fire else f'burst={burst}(发完即止)' if burst > 0 else f'duration={duration}s'}")
    print(f"  endpoint: {build_ws_url(deployment)}")
    if mode == "chat":
        print(f"  场景: 保险坐席×AI客户，mock 历史+坐席一轮≈1分钟话术，"
              f"{len(CHAT_SCENARIOS)} 个话本轮换"
              f"{'  [sync-fire: 全员注好历史后同一时刻齐发]' if sync_fire else ''}")
    if mode == "transcribe":
        if burst > 0:
            print(f"  transcription model: {transcribe_model}"
                  f"{f'  language={language}' if language else ''}"
                  f"  [burst: {concurrency} 条连接一口气发完 {burst} 个 commit，等全部结算]")
        else:
            print(f"  transcription model: {transcribe_model}"
                  f"{f'  language={language}' if language else ''}"
                  f"{f'  [管道化: 每连接 {pipeline} 在途，总在途 {concurrency * pipeline}]' if pipeline > 1 else '  [复用连接: 每 worker 一条 WS 循环转写]' if reuse_conn else ''}")
        if not reuse_conn and pipeline <= 1 and burst <= 0:
            print(f"  {_C['yellow']}提示: 未加 --reuse-conn，每次转写都新建 realtime 会话，"
                  f"429 可能来自 realtime 部署而非转写模型{_C['reset']}")

    stats = GlobalStats()
    stats.batch_index = batch_index
    stats.batch_concurrency = concurrency
    stop_event = asyncio.Event()
    sync = SyncFire(concurrency) if sync_fire else None
    # burst: 把总请求数均分给各连接（前 remainder 条多 1 个）
    def _share(i: int) -> int:
        if burst <= 0:
            return 0
        base, rem = divmod(burst, concurrency)
        return base + (1 if i < rem else 0)
    workers = [
        asyncio.create_task(
            worker_loop(stats, mode, deployment, stop_event, i, request_interval,
                        transcribe_model, language, reuse_conn, connect_stagger,
                        pipeline, _share(i), sync)
        )
        for i in range(concurrency)
    ]
    monitor = asyncio.create_task(monitor_loop(stats, stop_event))

    if sync is not None:
        # 等全员建连+注入历史（超时兜底：错峰总时长+60s），然后同一时刻开火。
        # 之后同 burst：等所有 worker 把这一轮结算完自然结束，忽略 duration。
        grace = connect_stagger * concurrency + 60
        try:
            await asyncio.wait_for(sync.all_ready.wait(), timeout=grace)
        except asyncio.TimeoutError:
            print(f"  {_C['yellow']}警告: 等就绪超时({grace:.0f}s)，"
                  f"仅 {sync.ready}/{concurrency} 路就绪，直接开火{_C['reset']}")
        sync.fired_at_utc = datetime.now(timezone.utc).isoformat()
        print(f"\n  {_C['bold']}⚡ 同步开火: {sync.ready}/{concurrency} 路就绪 "
              f"@ {sync.fired_at_utc}{_C['reset']}")
        sync.fire.set()
        await asyncio.gather(*workers, return_exceptions=True)
        stop_event.set()
    elif burst > 0:
        # burst 模式：发完即止，等所有 worker 把在途结算完自然结束
        await asyncio.gather(*workers, return_exceptions=True)
        stop_event.set()
    else:
        await asyncio.sleep(duration)
        stop_event.set()
        for w in workers:
            w.cancel()
    monitor.cancel()
    await asyncio.gather(*workers, monitor, return_exceptions=True)
    return stats


async def run_ramp_test(
    mode: str, deployment: str,
    ramp_start: int, ramp_max: int, ramp_step: int, step_duration: float,
    transcribe_model: str = "", language: str = "",
    reuse_conn: bool = False, connect_stagger: float = 0.0,
    pipeline: int = 1,
) -> tuple[list[dict], GlobalStats | None]:
    print(f"\n[Ramp] {ramp_start}→{ramp_max} 并发，每步 {step_duration}s")
    results = []
    last_stats = None
    concurrency = ramp_start
    batch_no = 0

    while concurrency <= ramp_max:
        batch_no += 1
        print(f"\n{'='*50}\n>>> 第 {batch_no} 批  并发: {concurrency}")
        stats = await run_load_test(mode, deployment, concurrency, step_duration,
                                    0.0, transcribe_model, language,
                                    batch_index=batch_no, reuse_conn=reuse_conn,
                                    connect_stagger=connect_stagger, pipeline=pipeline)
        last_stats = stats
        s = stats.summary()
        s["concurrency"] = concurrency
        s["batch"] = batch_no
        results.append(s)
        print(f"  RPM={s['avg_rpm']}  TPM={s['avg_tpm']}  "
              f"429s={s['rate_limited_429']}"
              f"(握手{s['rate_limited_handshake']}/会话{s['rate_limited_session']})"
              f"  P95={s['latency_p95_ms']}ms")

        if s["rate_limited_429"] > 0:
            attribution = ("模型/转写配额" if s["rate_limited_session"] > 0
                           else "连接级握手限流(非模型配额, 提高 --connect-stagger 或换 tier)")
            print(f"\n{_C['yellow']}!!! 并发={concurrency} 触发 429 "
                  f"[归属: {attribution}]，"
                  f"上一个稳定并发: {concurrency - ramp_step}{_C['reset']}")
            break
        concurrency += ramp_step

    print(f"\n{'='*70}\nRamp 汇总:")
    print(f"{'批':>3}  {'并发':>5}  {'RPM':>8}  {'TPM':>9}  {'429':>5}  "
          f"{'P95ms':>8}  {'首次异常':>10}")
    for r in results:
        fa = r.get("first_anomaly")
        anom = f"第{fa['seq']}个/{fa['kind']}" if fa else "—"
        mark = f" {_C['yellow']}<-- 限流{_C['reset']}" if r["rate_limited_429"] > 0 else ""
        print(f"{r.get('batch','?'):>3}  {r['concurrency']:>5}  {r['avg_rpm']:>8.1f}  "
              f"{r['avg_tpm']:>9.1f}  {r['rate_limited_429']:>5}  "
              f"{r['latency_p95_ms']:>8.1f}  {anom:>10}{mark}")

    # 找到全局首次异常所在批次
    for r in results:
        fa = r.get("first_anomaly")
        if fa:
            print(f"\n{_C['yellow']}{_C['bold']}⚑ 首次异常出现在 第 {fa['batch']} 批"
                  f"(并发 {fa['batch_cc']}) 第 {fa['seq']} 个请求 · {fa['worker']} · "
                  f"{fa['kind']} @ +{fa['elapsed']}s{_C['reset']}")
            break
    return results, last_stats


# ─── CLI ────────────────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Azure OpenAI Realtime API 压测工具 (GA)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--mode", choices=["text", "audio", "transcribe", "chat"], default="text",
                   help="text=文本补全  audio=语音对话  transcribe=纯转写(独立测 whisper 配额)  "
                        "chat=保险坐席多轮对话(mock 历史+坐席1分钟话术，AI 扮演客户)")
    p.add_argument("--sync-fire", action="store_true",
                   help="仅 chat 模式：所有 worker 先建连+注入完历史（握手仍按 "
                        "--connect-stagger 错峰），集合后同一时刻齐发 response.create，"
                        "打完这一轮即止(忽略 --duration)。测「同一时间戳 N 个并发请求」"
                        "打在模型配额上会怎样，如 --concurrency 100 --sync-fire")
    p.add_argument("--deployment",  default=DEPLOYMENT,
                   help="realtime 部署名(WS连接用)；transcribe 模式也连它")
    p.add_argument("--transcribe-model", default=WHISPER_DEPLOYMENT,
                   help="转写模型部署名(仅 transcribe 模式)，默认 $WHISPER_DEPLOYMENT")
    p.add_argument("--language",    default="",
                   help="转写语言 ISO-639-1(如 en/zh)，留空自动检测(仅 transcribe 模式)")
    p.add_argument("--reuse-conn",  action="store_true",
                   help="transcribe 模式复用 WS 连接：每 worker 建一次 realtime 会话，"
                        "在同一连接上循环 commit 转写。测 whisper 上限必开，否则 429 "
                        "会先撞 realtime 部署的会话创建限流")
    p.add_argument("--pipeline", type=int, default=1,
                   help="transcribe 模式每条连接的在途转写数(管道深度)，默认 1=串行。"
                        ">1 时不等上一个转写完成就连发 commit(隐含复用连接)，"
                        "总在途=并发×管道，才能真正打满转写模型配额。"
                        "如 --concurrency 10 --pipeline 10 = 100 在途，只需 10 次握手")
    p.add_argument("--burst", type=int, default=0,
                   help="burst 脉冲模式(仅 transcribe)：N 个转写请求按 --concurrency 均分，"
                        "每条连接一口气全部 commit，等全部结算后结束(忽略 --duration)。"
                        "如 --burst 1000 --concurrency 10 = 每连接瞬间灌 100 个在途，"
                        "看服务端在什么量级开始报什么错")
    p.add_argument("--connect-stagger", type=float, default=0.25,
                   help="worker 首次握手错峰间隔(秒/个)，默认 0.25。避免一批 worker "
                        "同时握手撞 S0 tier 连接建立速率(onHandshake 429)，把连接级 "
                        "429 和模型配额 429 混在一起。设 0 关闭(测握手上限时用)")
    p.add_argument("--concurrency", type=int,   default=5)
    p.add_argument("--duration",    type=float, default=60.0)
    p.add_argument("--interval",    type=float, default=0.0)
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--html",        action="store_true", help="生成 HTML 报告")

    # 对峙用：填入 Azure 承诺的配额
    p.add_argument("--expected-tpm", type=int, default=0,
                   help="Azure 承诺的 TPM 配额（用于报告对比）")
    p.add_argument("--expected-rpm", type=int, default=0,
                   help="Azure 承诺的 RPM 配额（用于报告对比）")
    p.add_argument("--region",       default="",
                   help="Azure 区域（如 eastus2），写入报告")

    # Ramp
    p.add_argument("--ramp",               action="store_true")
    p.add_argument("--ramp-start",         type=int,   default=1)
    p.add_argument("--ramp-max",           type=int,   default=50)
    p.add_argument("--ramp-step",          type=int,   default=5)
    p.add_argument("--ramp-step-duration", type=float, default=30.0)
    return p.parse_args()


def main() -> None:
    global LOG
    args = parse_args()

    if not ENDPOINT or not API_KEY:
        print("错误: 请设置 AZURE_OPENAI_ENDPOINT 和 AZURE_OPENAI_API_KEY")
        sys.exit(1)
    if not args.deployment:
        print("错误: 请设置 --deployment 或 REALTIME_DEPLOYMENT")
        sys.exit(1)
    if args.mode == "transcribe" and not args.transcribe_model:
        print("错误: transcribe 模式需要 --transcribe-model 或 WHISPER_DEPLOYMENT")
        sys.exit(1)
    if args.burst > 0 and args.mode != "transcribe":
        print("错误: --burst 仅支持 transcribe 模式")
        sys.exit(1)
    if args.burst > 0 and args.ramp:
        print("错误: --burst 与 --ramp 不能同时使用")
        sys.exit(1)
    if args.sync_fire and args.mode != "chat":
        print("错误: --sync-fire 仅支持 chat 模式")
        sys.exit(1)
    if args.sync_fire and args.ramp:
        print("错误: --sync-fire 与 --ramp 不能同时使用")
        sys.exit(1)

    LOG = EventLog(verbose=args.verbose)
    test_start_utc = datetime.now(timezone.utc).isoformat()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stats = None
    ramp_results = None

    try:
        if args.ramp:
            ramp_results, stats = loop.run_until_complete(
                run_ramp_test(
                    args.mode, args.deployment,
                    args.ramp_start, args.ramp_max,
                    args.ramp_step, args.ramp_step_duration,
                    args.transcribe_model, args.language,
                    reuse_conn=args.reuse_conn,
                    connect_stagger=args.connect_stagger,
                    pipeline=args.pipeline,
                )
            )
        else:
            stats = loop.run_until_complete(
                run_load_test(
                    args.mode, args.deployment,
                    args.concurrency, args.duration, args.interval,
                    args.transcribe_model, args.language,
                    reuse_conn=args.reuse_conn,
                    connect_stagger=args.connect_stagger,
                    pipeline=args.pipeline,
                    burst=args.burst,
                    sync_fire=args.sync_fire,
                )
            )
            s = stats.summary()
            print(f"\n{'='*60}\n{_C['bold']}最终统计:{_C['reset']}")
            for k, v in s.items():
                print(f"  {k:<26}: {v}")
            fa = stats.first_anomaly
            if fa:
                print(f"\n{_C['yellow']}{_C['bold']}⚑ 首次异常: 第 {fa['seq']} 个请求"
                      f" · {fa['worker']} · {fa['kind']} @ +{fa['elapsed']}s{_C['reset']}")
            if stats.errors:
                print(f"\n{_C['red']}错误样本 (最近10条):{_C['reset']}")
                for e in stats.errors[-10:]:
                    print(f"  {e}")

    except KeyboardInterrupt:
        print("\n用户中断")
    finally:
        if args.html and stats:
            s = stats.summary()
            meta = {
                "test_start_utc":  test_start_utc,
                "endpoint":        ENDPOINT,
                "deployment":      args.deployment,
                "transcribe_model": args.transcribe_model if args.mode == "transcribe" else "",
                "reuse_conn":      args.reuse_conn,
                "connect_stagger": args.connect_stagger,
                "pipeline":        args.pipeline,
                "burst":           args.burst,
                "sync_fire":       args.sync_fire,
                "region":          args.region,
                "mode":            args.mode,
                "concurrency":     args.concurrency,
                "duration_s":      args.duration,
                "expected_tpm":    args.expected_tpm,
                "expected_rpm":    args.expected_rpm,
                "summary":         s,
                "rate_limit_details": stats.rate_limit_details,
                "rate_limit_reports": stats.rate_limit_reports,
                "timeseries":      stats.timeseries,
                "ramp_results":    ramp_results or [],
            }
            ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = f"realtime_report_{args.mode}_c{args.concurrency}_{ts}.html"
            path = os.path.join(os.path.dirname(os.path.abspath(__file__)), name)
            LOG.write_html(path, meta)
        loop.close()


# ─── HTML 报告模板 ──────────────────────────────────────────────────────────────
_HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>Azure Realtime Loadtest Report</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"SF Mono","Fira Code",monospace;font-size:12px;background:#0d1117;color:#c9d1d9}
h2{font-size:14px;color:#8b949e;margin-bottom:10px;font-weight:normal;text-transform:uppercase;letter-spacing:.05em}
/* ── 顶部元信息 ── */
#meta{background:#161b22;border-bottom:1px solid #30363d;padding:14px 18px;display:flex;flex-wrap:wrap;gap:6px 20px}
#meta .kv{display:flex;gap:6px}
#meta .k{color:#8b949e}
#meta .v{color:#79c0ff}
/* ── 布局 ── */
.section{padding:16px 18px;border-bottom:1px solid #21262d}
/* ── 摘要卡片 ── */
#cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:10px}
.card{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:12px}
.card .label{color:#8b949e;font-size:11px;margin-bottom:4px}
.card .value{font-size:22px;font-weight:bold;color:#c9d1d9}
.card .sub{color:#8b949e;font-size:11px;margin-top:2px}
.card.warn .value{color:#d29922}
.card.danger .value{color:#f85149}
.card.good .value{color:#56d364}
/* ── 异常分类 / token 明细 chips ── */
#anom-chips,#usage-chips{display:flex;flex-wrap:wrap;gap:10px}
.chip{background:#161b22;border:1px solid #30363d;border-radius:20px;padding:6px 14px;
  display:flex;gap:8px;align-items:center}
.chip .n{font-size:16px;font-weight:bold}
.chip .lbl{color:#8b949e;font-size:12px}
.chip.zero{opacity:.45}
/* ── 图表 ── */
#chart-wrap{position:relative;height:260px;margin-top:4px}
#chart{width:100%;height:100%}
/* ── 429 表 / rate_limits 表 ── */
#rl-table,#rlr-table{width:100%;border-collapse:collapse}
#rl-table th,#rl-table td,#rlr-table th,#rlr-table td{padding:5px 10px;text-align:left;border-bottom:1px solid #21262d}
#rl-table th,#rlr-table th{color:#8b949e;font-weight:normal;background:#161b22}
#rl-table td.t{color:#d29922}
#declared-chips{display:flex;flex-wrap:wrap;gap:10px}
/* ── 工具栏 ── */
#toolbar{position:sticky;top:0;background:#161b22;padding:8px 12px;display:flex;gap:8px;
  flex-wrap:wrap;z-index:10;border-bottom:1px solid #30363d}
#toolbar input,#toolbar select{background:#0d1117;color:#c9d1d9;border:1px solid #30363d;
  border-radius:4px;padding:4px 8px;font-size:12px}
#toolbar input{width:200px}
#count,#export-btn{color:#8b949e;align-self:center;cursor:pointer}
#export-btn{margin-left:auto;background:#21262d;border:1px solid #30363d;
  border-radius:4px;padding:4px 10px;color:#79c0ff}
#export-btn:hover{background:#30363d}
/* ── 日志表 ── */
table.log{width:100%;border-collapse:collapse}
table.log th{position:sticky;top:41px;background:#161b22;color:#8b949e;font-weight:normal;
  text-align:left;padding:4px 8px;border-bottom:1px solid #21262d;white-space:nowrap}
table.log tr:hover{background:#161b22}
table.log td{padding:3px 8px;border-bottom:1px solid #21262d;vertical-align:top}
.ts{color:#8b949e;white-space:nowrap}
.wid{color:#79c0ff;white-space:nowrap}
.bt{color:#58a6ff;white-space:nowrap}
.sq{color:#d29922;white-space:nowrap}
.dir{text-align:center}
.s{color:#56d364}.r{color:#58a6ff}.ok{color:#56d364;font-weight:bold}
.er{color:#f85149}.rl{color:#d29922}.i{color:#8b949e}
.lvl-INFO{color:#c9d1d9}.lvl-WARN{color:#d29922}.lvl-ERROR{color:#f85149}.lvl-DEBUG{color:#6e7681}
tr.first-anom{background:#3d1a1a !important;outline:1px solid #f85149}
tr.first-anom td.evt{color:#ff7b72;font-weight:bold}
.detail{color:#8b949e;word-break:break-all}
.errstr{color:#f85149}
td.dir{width:20px}td.ts{width:96px}td.wid{width:50px}td.bt{width:34px}td.sq{width:44px}
td.evt{width:220px;white-space:nowrap}
</style>
</head>
<body>
<div id="meta"></div>

<div class="section">
  <h2>关键指标</h2>
  <div id="cards"></div>
</div>

<div class="section" id="anom-section">
  <h2>异常分类</h2>
  <div id="anom-chips"></div>
</div>

<div class="section" id="usage-section" style="display:none">
  <h2>Token 消耗明细 (usage.input/output_token_details)</h2>
  <div id="usage-chips"></div>
</div>

<div class="section" id="declared-section" style="display:none">
  <h2>Azure 上报的配额 (rate_limits.updated 事件)</h2>
  <div id="declared-chips"></div>
  <table id="rlr-table" style="margin-top:10px">
    <thead><tr>
      <th>+时间(s)</th><th>名称</th><th>limit</th><th>remaining</th><th>reset(s)</th>
    </tr></thead>
    <tbody id="rlr-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>RPM / TPM 时序</h2>
  <div id="chart-wrap"><canvas id="chart"></canvas></div>
</div>

<div class="section" id="rl-section" style="display:none">
  <h2>429 限流详情</h2>
  <table id="rl-table">
    <thead><tr>
      <th>+时间(s)</th><th>批</th><th>#序号</th><th>Worker</th><th>来源</th>
      <th>RPM@事件</th><th>TPM@事件</th>
      <th>错误码</th><th>Retry-After</th><th>错误信息</th>
    </tr></thead>
    <tbody id="rl-body"></tbody>
  </table>
</div>

<div class="section">
  <h2>事件日志</h2>
  <div id="toolbar">
    <input id="search" placeholder="搜索事件/详情/错误..." oninput="renderLog()">
    <select id="fLevel" onchange="renderLog()">
      <option value="">全部级别</option>
      <option>INFO</option><option>WARN</option><option>ERROR</option><option>DEBUG</option>
    </select>
    <select id="fBatch" onchange="renderLog()"></select>
    <select id="fWorker" onchange="renderLog()"></select>
    <select id="fDir" onchange="renderLog()">
      <option value="">全部方向</option>
      <option value="→">→ 发送</option><option value="←">← 接收</option>
      <option value="✓">✓ 成功</option><option value="✗">✗ 错误</option>
      <option value="⚡">⚡ 限流</option>
    </select>
    <label style="align-self:center;color:#8b949e">
      <input type="checkbox" id="fAnomaly" onchange="renderLog()"> 仅异常
    </label>
    <span id="count"></span>
    <button id="export-btn" onclick="exportCSV()">⬇ 导出 CSV</button>
  </div>
  <table class="log">
    <thead><tr>
      <th>时间</th><th>+秒</th><th>批</th><th>#</th><th>Worker</th><th>方向</th>
      <th>事件</th><th>详情</th><th>错误</th>
    </tr></thead>
    <tbody id="tbody"></tbody>
  </table>
</div>

<script>
const DATA  = __PAYLOAD__;
const META  = DATA.meta;
const ROWS  = DATA.rows;
const SUM   = META.summary || {};
const TS    = META.timeseries || [];
const RL    = META.rate_limit_details || [];
const RAMP  = META.ramp_results || [];

// ── 元信息条 ──────────────────────────────────────────────────────────────────
const metaFields = [
  ["时间",     META.test_start_utc],
  ["Endpoint", META.endpoint],
  ["部署",     META.deployment],
  ["区域",     META.region || "—"],
  ["模式",     META.mode],
  ...(META.transcribe_model ? [["转写模型", META.transcribe_model]] : []),
  ...(META.mode === "transcribe" ? [["复用连接", (META.pipeline>1 || META.reuse_conn) ? "是" : "否(429或含握手限流)"]] : []),
  ...(META.pipeline > 1 ? [["管道深度", META.pipeline + "/连接 (总在途 " + (META.pipeline * META.concurrency) + ")"]] : []),
  ...(META.burst > 0 ? [["burst", META.burst + " 个一次发完"]] : []),
  ...(META.connect_stagger ? [["握手错峰", META.connect_stagger + "s/个"]] : []),
  ["并发",     META.concurrency],
  ["时长",     META.duration_s + "s"],
  ["期望TPM",  META.expected_tpm || "—"],
  ["期望RPM",  META.expected_rpm || "—"],
];
document.getElementById("meta").innerHTML =
  metaFields.map(([k,v]) => `<div class="kv"><span class="k">${k}:</span><span class="v">${esc(String(v||""))}</span></div>`).join("");

// ── 摘要卡片 ──────────────────────────────────────────────────────────────────
function pct(n,d){return d?((n/d)*100).toFixed(1)+"%":"—"}
const quota_rpm = META.expected_rpm || 0;
const quota_tpm = META.expected_tpm || 0;
const peak_rpm  = SUM.peak_rpm || 0;
const peak_tpm  = SUM.peak_tpm || 0;
const ok_rate   = SUM.success_rate_pct || 0;
const f429      = SUM.first_429_elapsed_s;
const FA        = SUM.first_anomaly;   // {batch,batch_cc,seq,worker,elapsed,kind}
// whisper 家族按音频时长计费(无 token)：Token/TPM 卡片换成时长指标
const durMode   = (SUM.total_tokens||0) === 0 && (SUM.transcribed_audio_s||0) > 0;
const faText    = FA ? `第${FA.batch}批(并发${FA.batch_cc}) 第${FA.seq}个 · ${FA.worker}` : "—";
const faSub     = FA ? `${FA.kind} @ +${FA.elapsed}s` : "全程无异常";

function quotaLine(peak, quota, unit) {
  if (!quota) return "";
  const ratio = (peak/quota*100).toFixed(1);
  const cls   = ratio >= 90 ? "color:#f85149" : ratio >= 70 ? "color:#d29922" : "color:#56d364";
  return `期望${quota.toLocaleString()} ${unit}，实测 <span style="${cls}">${ratio}%</span>`;
}

const cards = [
  {label:"总请求数",   value: (SUM.total_requests||0).toLocaleString(), sub:"",       cls:""},
  {label:"成功率",     value: ok_rate+"%",                              sub:`成功 ${(SUM.success||0).toLocaleString()} 次`, cls: ok_rate>=95?"good":ok_rate>=80?"warn":"danger"},
  {label:"峰值 RPM",   value: peak_rpm.toLocaleString(),               sub: quotaLine(peak_rpm, quota_rpm, "RPM"),    cls: quota_rpm&&peak_rpm<quota_rpm*0.7?"danger":""},
  durMode
    ? {label:"转写速率", value: (SUM.avg_audio_s_per_min||0)+"s/min", sub:"每分钟转写的音频秒数(whisper 按时长计费)", cls:""}
    : {label:"峰值 TPM", value: peak_tpm.toLocaleString(),            sub: quotaLine(peak_tpm, quota_tpm, "TPM"),    cls: quota_tpm&&peak_tpm<quota_tpm*0.7?"danger":""},
  {label:"429 次数",   value: (SUM.rate_limited_429||0).toLocaleString(),
   sub: SUM.rate_limited_429>0
     ? `握手(连接级) ${SUM.rate_limited_handshake||0} · 会话内(模型配额) ${SUM.rate_limited_session||0}${f429!=null?` · 首次 +${f429}s`:""}`
     : "未触发",
   cls: SUM.rate_limited_429>0?"danger":"good"},
  {label:"首次异常(第几批第几个)", value: faText,                        sub: faSub,  cls: FA?"danger":"good"},
  {label:"首次限流时",  value: f429!=null?"+"+f429+"s":"—",            sub: f429!=null?`RPM=${SUM.first_429_rpm} TPM=${SUM.first_429_tpm}`:"",  cls: f429!=null?"danger":""},
  {label:"P95 延迟",   value: (SUM.latency_p95_ms||0)+"ms",           sub:`P99=${SUM.latency_p99_ms||0}ms Max=${SUM.latency_max_ms||0}ms`, cls:""},
  durMode
    ? {label:"转写音频总时长", value: (SUM.transcribed_audio_s||0)+"s", sub:`≈ ${((SUM.transcribed_audio_s||0)/60).toFixed(1)} 分钟 · whisper 无 token 计数`, cls:""}
    : {label:"总 Token", value: (SUM.total_tokens||0).toLocaleString(), sub:`in=${(SUM.input_tokens||0).toLocaleString()} out=${(SUM.output_tokens||0).toLocaleString()}`, cls:""},
];
document.getElementById("cards").innerHTML = cards.map(c =>
  `<div class="card ${c.cls}">
    <div class="label">${c.label}</div>
    <div class="value">${c.value}</div>
    ${c.sub?`<div class="sub">${c.sub}</div>`:""}
  </div>`).join("");

// ── 异常分类 chips ────────────────────────────────────────────────────────────
const e429   = SUM.rate_limited_429 || 0;
const e429hs = SUM.rate_limited_handshake || 0;
const e429ss = SUM.rate_limited_session || 0;
const eTrans = SUM.transcription_failed || 0;
const eResp  = SUM.response_failed || 0;
const eTimeout = SUM.timeouts || 0;
const eConn  = SUM.connection_errors || 0;
const eOther = Math.max(0, (SUM.failed||0) - e429 - eTrans - eResp - eTimeout);
const chips = [
  {lbl:"429 握手(连接级)",   n:e429hs, color:"#f85149"},
  {lbl:"429 会话内(模型配额)", n:e429ss, color:"#ff7b72"},
  {lbl:"转写失败",       n:eTrans,  color:"#db6d28"},
  {lbl:"response失败",   n:eResp,   color:"#f0883e"},
  {lbl:"超时",           n:eTimeout,color:"#d29922"},
  {lbl:"连接错误",       n:eConn,   color:"#bc8cff"},
  {lbl:"其他失败",       n:eOther,  color:"#8b949e"},
];
const anomTotal = e429+eTrans+eResp+eTimeout+eConn+eOther;
document.getElementById("anom-chips").innerHTML = chips.map(c =>
  `<div class="chip ${c.n?'':'zero'}">
     <span class="n" style="color:${c.color}">${c.n}</span>
     <span class="lbl">${c.lbl}</span>
   </div>`).join("");
if (anomTotal === 0)
  document.getElementById("anom-chips").innerHTML =
    '<div class="chip zero"><span class="lbl">全程无异常 ✓</span></div>';

// ── Token 消耗明细 (cached 是 input 的子集，不重复计入总数) ────────────────────
const UD = SUM.usage_details || {};
const udItems = [
  ["输入·文本",     UD.input_text_tokens,   "#79c0ff"],
  ["输入·音频",     UD.input_audio_tokens,  "#79c0ff"],
  ["输入·图像",     UD.input_image_tokens,  "#79c0ff"],
  ["输入·缓存命中", UD.input_cached_tokens, "#56d364"],
  ["缓存·文本",     UD.cached_text_tokens,  "#56d364"],
  ["缓存·音频",     UD.cached_audio_tokens, "#56d364"],
  ["缓存·图像",     UD.cached_image_tokens, "#56d364"],
  ["输出·文本",     UD.output_text_tokens,  "#d2a8ff"],
  ["输出·音频",     UD.output_audio_tokens, "#d2a8ff"],
].filter(([,n]) => n > 0);
if (udItems.length) {
  document.getElementById("usage-section").style.display = "";
  document.getElementById("usage-chips").innerHTML = udItems.map(([lbl,n,color]) =>
    `<div class="chip">
       <span class="n" style="color:${color}">${n.toLocaleString()}</span>
       <span class="lbl">${lbl}</span>
     </div>`).join("");
}

// ── Azure 上报配额 (rate_limits.updated) ──────────────────────────────────────
const RLR      = META.rate_limit_reports || [];
const DECLARED = SUM.declared_limits || {};
if (RLR.length > 0 || Object.keys(DECLARED).length > 0) {
  document.getElementById("declared-section").style.display = "";
  document.getElementById("declared-chips").innerHTML =
    Object.entries(DECLARED).map(([name,d])=>
      `<div class="chip">
         <span class="lbl">${esc(name)} 上限</span>
         <span class="n" style="color:#58a6ff">${d.limit!=null?d.limit.toLocaleString():"—"}</span>
         <span class="lbl">最低剩余</span>
         <span class="n" style="color:${d.min_remaining===0?'#f85149':'#56d364'}">${d.min_remaining!=null?d.min_remaining.toLocaleString():"—"}</span>
       </div>`).join("");
  document.getElementById("rlr-body").innerHTML = RLR.map(r=>
    `<tr>
       <td>+${r.elapsed}s</td><td>${esc(r.name)}</td>
       <td>${r.limit!=null?r.limit.toLocaleString():"—"}</td>
       <td style="color:${r.remaining===0?'#f85149':'#c9d1d9'}">${r.remaining!=null?r.remaining.toLocaleString():"—"}</td>
       <td>${r.reset_seconds!=null?r.reset_seconds:"—"}</td>
     </tr>`).join("");
}

// ── 429 详情表 ────────────────────────────────────────────────────────────────
if (RL.length > 0) {
  document.getElementById("rl-section").style.display = "";
  document.getElementById("rl-body").innerHTML = RL.map(r =>
    `<tr>
      <td class="t">+${r.elapsed}s</td>
      <td style="color:#58a6ff">B${r.batch||"?"}</td>
      <td style="color:#d29922">#${r.seq||"?"}</td>
      <td>${esc(r.worker||"—")}</td>
      <td>${r.source==="handshake"
            ? '<span style="color:#bc8cff">握手/连接级</span>'
            : '<span style="color:#f85149">会话内/模型配额</span>'}</td>
      <td>${r.rpm}</td><td>${r.tpm}</td>
      <td style="color:#f85149">${esc(r.code)}</td>
      <td>${esc(r.retry_after||"—")}</td>
      <td class="detail">${esc(r.message)}</td>
    </tr>`).join("");
}

// ── 时序图 ────────────────────────────────────────────────────────────────────
(function drawChart(){
  const canvas = document.getElementById("chart");
  const W = canvas.offsetWidth, H = canvas.offsetHeight;
  canvas.width = W; canvas.height = H;
  const ctx = canvas.getContext("2d");
  if (!TS.length) { ctx.fillStyle="#8b949e"; ctx.fillText("无时序数据",W/2-40,H/2); return; }

  const PAD = {top:20, right:80, bottom:36, left:70};
  const cw = W - PAD.left - PAD.right;
  const ch = H - PAD.top  - PAD.bottom;

  const maxElapsed = Math.max(...TS.map(d=>d.elapsed), 1);
  const maxRPM     = Math.max(...TS.map(d=>d.rpm), quota_rpm, 1);
  const maxTPM     = Math.max(...TS.map(d=>d.tpm), quota_tpm, 1);

  function xp(e){return PAD.left + e/maxElapsed*cw}
  function yRPM(v){return PAD.top + ch - v/maxRPM*ch}
  function yTPM(v){return PAD.top + ch - v/maxTPM*ch}

  // 背景格线
  ctx.strokeStyle="#21262d"; ctx.lineWidth=1;
  for(let i=0;i<=5;i++){
    const y = PAD.top + i/5*ch;
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
  }

  // 期望配额横线
  if(quota_rpm){
    ctx.setLineDash([6,4]); ctx.strokeStyle="#d29922"; ctx.lineWidth=1.2;
    const y = yRPM(quota_rpm);
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
    ctx.fillStyle="#d29922"; ctx.font="10px monospace";
    ctx.fillText("RPM quota "+quota_rpm, PAD.left+4, y-4);
  }
  if(quota_tpm){
    ctx.setLineDash([6,4]); ctx.strokeStyle="#e3b341"; ctx.lineWidth=1.2;
    const y = yTPM(quota_tpm);
    ctx.beginPath(); ctx.moveTo(PAD.left,y); ctx.lineTo(PAD.left+cw,y); ctx.stroke();
    ctx.fillStyle="#e3b341"; ctx.font="10px monospace";
    ctx.fillText("TPM quota "+quota_tpm.toLocaleString(), PAD.left+4, y-4);
  }
  ctx.setLineDash([]);

  // 429 事件竖线
  RL.forEach(r=>{
    const x = xp(r.elapsed);
    ctx.strokeStyle="rgba(248,81,73,0.6)"; ctx.lineWidth=1.5;
    ctx.beginPath(); ctx.moveTo(x, PAD.top); ctx.lineTo(x, PAD.top+ch); ctx.stroke();
    ctx.fillStyle="#f85149"; ctx.font="10px monospace";
    ctx.fillText("429", x+3, PAD.top+12);
  });

  // RPM 线（蓝）
  ctx.strokeStyle="#58a6ff"; ctx.lineWidth=2;
  ctx.beginPath();
  TS.forEach((d,i)=>{
    i===0 ? ctx.moveTo(xp(d.elapsed),yRPM(d.rpm)) : ctx.lineTo(xp(d.elapsed),yRPM(d.rpm));
  });
  ctx.stroke();

  // TPM 线（绿，右轴）
  ctx.strokeStyle="#56d364"; ctx.lineWidth=2;
  ctx.beginPath();
  TS.forEach((d,i)=>{
    i===0 ? ctx.moveTo(xp(d.elapsed),yTPM(d.tpm)) : ctx.lineTo(xp(d.elapsed),yTPM(d.tpm));
  });
  ctx.stroke();

  // 轴标签
  ctx.fillStyle="#8b949e"; ctx.font="10px monospace";
  // X轴
  for(let i=0;i<=4;i++){
    const e = maxElapsed*i/4;
    ctx.fillText(Math.round(e)+"s", xp(e)-8, PAD.top+ch+14);
  }
  // Y轴左（RPM）
  ctx.fillStyle="#58a6ff";
  for(let i=0;i<=4;i++){
    const v = maxRPM*i/4;
    ctx.fillText(Math.round(v), 4, yRPM(v)+4);
  }
  ctx.save(); ctx.translate(14, PAD.top+ch/2); ctx.rotate(-Math.PI/2);
  ctx.fillText("RPM", -12, 0); ctx.restore();
  // Y轴右（TPM）
  ctx.fillStyle="#56d364";
  for(let i=0;i<=4;i++){
    const v = maxTPM*i/4;
    ctx.fillText(Math.round(v), PAD.left+cw+4, yTPM(v)+4);
  }
  ctx.save(); ctx.translate(W-6, PAD.top+ch/2); ctx.rotate(Math.PI/2);
  ctx.fillText("TPM", -12, 0); ctx.restore();

  // 图例
  const lx = PAD.left+10, ly = PAD.top+6;
  [[" RPM","#58a6ff"],[" TPM","#56d364"],["— 配额","#d29922"],["↑ 429","#f85149"]].forEach(([label,color],i)=>{
    ctx.fillStyle=color;
    ctx.fillRect(lx+i*80, ly, 10, 10);
    ctx.fillStyle="#c9d1d9";
    ctx.fillText(label, lx+i*80+12, ly+9);
  });
})();

// ── 日志表 ───────────────────────────────────────────────────────────────────
const workers = [...new Set(ROWS.map(r=>r.worker))].sort();
const wSel = document.getElementById("fWorker");
wSel.innerHTML = '<option value="">全部 Worker</option>' +
  workers.map(w=>`<option>${w}</option>`).join("");

const batches = [...new Set(ROWS.map(r=>r.batch))].sort((a,b)=>a-b);
const bSel = document.getElementById("fBatch");
bSel.innerHTML = '<option value="">全部批次</option>' +
  batches.map(b=>`<option value="${b}">第 ${b} 批</option>`).join("");

// 首次异常行标记（batch+seq+worker 定位）
const isAnomalyRow = r => r.direction==="⚡" || r.direction==="✗" || r.level==="ERROR";

const DIR_CLASS = {"→":"s","←":"r","✓":"ok","✗":"er","⚡":"rl","·":"i"};
function renderLog(){
  const q       = document.getElementById("search").value.toLowerCase();
  const lvl     = document.getElementById("fLevel").value;
  const wid     = document.getElementById("fWorker").value;
  const bat     = document.getElementById("fBatch").value;
  const dir     = document.getElementById("fDir").value;
  const anomOnly= document.getElementById("fAnomaly").checked;
  const rows = ROWS.filter(r=>
    (!lvl||r.level===lvl)&&(!wid||r.worker===wid)&&
    (bat===""||String(r.batch)===bat)&&
    (!dir||r.direction===dir)&&
    (!anomOnly||isAnomalyRow(r))&&
    (!q||(r.event+r.detail+r.error+r.worker).toLowerCase().includes(q))
  );
  document.getElementById("count").textContent = `${rows.length}/${ROWS.length} 条`;
  document.getElementById("tbody").innerHTML = rows.map(r=>{
    const fa = FA && r.batch===FA.batch && r.seq===FA.seq && r.worker===FA.worker
               && isAnomalyRow(r);
    return `
    <tr class="lvl-${r.level}${fa?' first-anom':''}">
      <td class="ts">${r.ts}</td>
      <td class="ts">+${r.elapsed}s</td>
      <td class="bt">${r.batch?('B'+r.batch):''}</td>
      <td class="sq">${r.seq?('#'+r.seq):''}</td>
      <td class="wid">${r.worker}</td>
      <td class="dir ${DIR_CLASS[r.direction]||""}">${r.direction}</td>
      <td class="evt">${fa?'⚑ ':''}${r.event}</td>
      <td class="detail">${esc(r.detail)}</td>
      <td class="errstr">${esc(r.error)}</td>
    </tr>`;}).join("");
}
renderLog();

// ── CSV 导出 ─────────────────────────────────────────────────────────────────
function exportCSV(){
  const hdr = ["elapsed","ts","batch","batch_cc","seq","level","worker","direction","event","detail","error"];
  const lines = [hdr.join(",")];
  ROWS.forEach(r=>{
    lines.push(hdr.map(k=>'"'+String(r[k]===undefined?"":r[k]).replace(/"/g,'""')+'"').join(","));
  });
  // 首次异常
  if (FA) {
    lines.push("","# 首次异常","batch,batch_cc,seq,worker,kind,elapsed");
    lines.push([FA.batch,FA.batch_cc,FA.seq,FA.worker,FA.kind,FA.elapsed].join(","));
  }
  // 也附上 429 详情
  lines.push("","# 429 详情","elapsed,batch,seq,worker,source,code,message,retry_after,rpm,tpm");
  RL.forEach(r=>{
    lines.push([r.elapsed,r.batch,r.seq,r.worker,r.source||"",r.code,'"'+r.message.replace(/"/g,'""')+'"',r.retry_after,r.rpm,r.tpm].join(","));
  });
  // 时序数据
  lines.push("","# 时序","elapsed,rpm,tpm,ok,e429,err");
  TS.forEach(r=>{
    lines.push([r.elapsed,r.rpm,r.tpm,r.ok,r.e429,r.err].join(","));
  });
  const blob = new Blob([lines.join("\n")],{type:"text/csv;charset=utf-8"});
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = "realtime_loadtest.csv";
  a.click();
}

function esc(s){
  return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
