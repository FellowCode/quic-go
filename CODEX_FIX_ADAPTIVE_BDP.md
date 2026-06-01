# Codex task: исправить и усилить `AdaptiveBDP` в `FellowCode/quic-go`

Репозиторий: `https://github.com/FellowCode/quic-go`

Цель этого задания: исправить ошибки в текущей реализации `CongestionControlAdaptiveBDP`, из-за которых upload может самозадушиться без loss, и довести алгоритм до поведения:

- при постоянном потоке данных канал раскрывается достаточно быстро;
- примерный целевой сценарий: `30 Mbps`, RTT около `150 ms`, no loss — выйти на рабочий rate за несколько секунд и не падать в минимум;
- при реальном падении bottleneck bandwidth алгоритм быстро снижает `pacing_rate` и постепенно поджимает `cwnd`, не создавая долгой очереди;
- app-limited и sender/pacer-limited samples не должны ошибочно снижать bandwidth estimate;
- persistent congestion всё ещё должен снижать `cwnd` до minimum window.

## 0. Зафиксированный симптом

Диагностика с клиента:

```text
Окно теста: примерно 10:40:27-10:41:30 локально.
loss: lost_packets=0, bytes_lost=0
RTT: smoothed_rtt ~148-150ms, стабильно
DNS/reconnect: явных ошибок нет

В начале upload:
  cwnd_bytes ~= 724823
  bytes_in_flight ~= 500 KB
  заполнение ~= 69%

Около 10:40:37:
  cwnd_bytes резко стал 40960
  40960 = 32 packets * 1280 bytes

После этого:
  bytes_in_flight обычно 2882-4323 bytes
  tx_write_pending=20480
  tx_app_limited=false
  tx_cwnd_underfill=true
  stream.Write зависал на записи 20 KB по 3.4-8.1s
```

Использованный конфиг:

```go
CwndTuning: quic.CwndTuning{
    Enable: true,
    Algorithm: quic.CongestionControlAdaptiveBDP,
    InitialWindowPackets: 256,
    MinWindowPackets:     32,
    StartupTargetRateBps: 30_000_000, // 30 Mbps
}
```

Вывод: `40960` — это не случайность, а ровно `MinWindowPackets * MSS`. Алгоритм срезал `cwnd` до `minCongestionWindow`, хотя потерь не было, RTT был стабилен, данные были, `tx_app_limited=false`.

Для `30 Mbps` и RTT `150 ms` рабочий BDP:

```text
30_000_000 / 8 * 0.150 = 562_500 bytes
562_500 / 1280 ~= 439 packets

cwnd_gain=1.5 => target_cwnd ~= 843_750 bytes ~= 659 packets
cwnd_gain=2.0 => target_cwnd ~= 1_125_000 bytes ~= 879 packets
```

Значит при таком сценарии `cwnd=40960` — это явный collapse, а не корректная адаптация.

---

## 1. Главные вероятные причины

### 1.1. Некорректный / неполный delivery-rate sampler

Текущая логика в `internal/ackhandler/sent_packet_handler.go` примерно такая:

```go
p.DeliveredBytes = h.deliveredBytes
p.DeliveredTime = h.deliveredTime
p.FirstSentTime = h.firstSentTime

if h.bytesInFlight == 0 {
    h.firstSentTime = t
    p.FirstSentTime = t
}
```

В `makeRateSample` используется:

```go
delivered := h.deliveredBytes - p.DeliveredBytes
ackElapsed := rcvTime.Sub(p.DeliveredTime)
sendElapsed := p.SendTime.Sub(p.FirstSentTime)
interval := max(ackElapsed, sendElapsed)
delivery_rate := delivered / interval
```

Проблема: `h.firstSentTime` обновляется при отправке из idle, но не обновляется при ACK newest delivered packet. На долгом continuous upload `bytesInFlight` может долго не становиться нулём, поэтому `p.FirstSentTime` остаётся слишком старым. Тогда `sendElapsed` растёт до секунд, delivery sample искусственно занижается, `bw` падает, `pacing_rate` падает, `target_cwnd` падает, `cwnd` может схлопнуться до `MinWindowPackets * MSS`.

### 1.2. `canReduceWindow` смешивает разные условия

Текущая функция:

```go
func (s *adaptiveBDPSender) canReduceWindow(priorInFlight protocol.ByteCount) bool {
    return s.queueDelay() > s.queueTarget() && priorInFlight < s.congestionWindow
}
```

Она используется сразу для нескольких разных решений:

- вход в `ProbeDown` по queue pressure;
- downshift по низкому delivery-rate sample;
- loss reaction;
- ECN reaction;
- уменьшение `cwnd` к `target_cwnd`.

Это опасно. Loss/ECN не должны зависеть от такого gate. Downshift по bandwidth drop не должен зависеть только от `queueDelay > queueTarget`. Уменьшение `cwnd` не должно instant-collapse до minimum из-за одного класса сигнала.

### 1.3. Downshift может сработать от self-limited sample

Если `pacing_rate` уже ошибочно снижен, sender сам начинает отправлять мало. Следующие delivery samples показывают низкую скорость не потому, что bottleneck низкий, а потому что алгоритм сам себя ограничил. Такие samples нельзя использовать для снижения `shortBw` / `bw`.

Нужно различать:

```text
app-limited       => приложение не даёт данных;
sender-limited    => алгоритм сам ограничил отправку cwnd/pacer;
pipe-filled sample => sender реально пытался заполнить bottleneck pipe.
```

Downshift разрешён только от валидного, non-app-limited, pipe-filled sample.

### 1.4. `ProbeDown` выходит слишком легко

Сейчас выход из `ProbeDown` допускается, если выполнено любое из условий:

```go
priorInFlight <= s.bdp() ||
s.queueDelay() <= s.queueTarget()/2 ||
eventTime.Sub(s.lastStateChange) >= minDrain
```

Это значит: через один RTT можно выйти из `ProbeDown`, даже если очередь не дренирована и inflight всё ещё выше BDP. Правильнее требовать минимум один RTT **и** факт drain.

### 1.5. `shouldEnterProbeDown` вызывается дважды на один ACK

В `OnPacketAckedWithRateSample` сейчас есть паттерн:

```go
if s.shouldEnterProbeDown(sample, priorInFlight) {
    s.enterState(adaptiveBDPProbeDown, eventTime)
}

if s.state == adaptiveBDPStartup && (s.fullBwReached || s.shouldEnterProbeDown(sample, priorInFlight)) {
    s.enterState(adaptiveBDPDrain, eventTime)
}
```

Если `shouldEnterProbeDown` мутирует `queueHighRounds`, один ACK может засчитаться дважды. Нужно вычислять decision один раз.

### 1.6. Round tracking использует `lastStateChange` как fallback timer

`updateRound` использует `lastStateChange` для time-based fallback. Это хрупко: смена state не равна началу round. Нужен отдельный `lastRoundStartTime`.

---

## 2. Правильные инварианты алгоритма

Сохрани эти правила во всей реализации:

```text
BDP model:
    bdp_bytes = bw_bytes_per_sec * min_rtt_seconds
    target_cwnd = cwnd_gain * bdp_bytes * window_gain
    pacing_rate = pacing_gain * bw_bytes_per_sec * (1 - pacing_margin)

Never:
    GetCongestionWindow() = max(cwnd, bdpTarget)

Do:
    cwnd движется к target_cwnd через ACK/loss/queue state;
    pacing_rate обновляется сразу при изменении bw estimate;
    при реальном падении bandwidth pacing_rate снижается быстро;
    cwnd может быть ниже bytes_in_flight, чтобы временно остановить новые sends и дать flight дренироваться;
    app-limited samples не снижают maxBw/shortBw;
    sender-limited samples не снижают shortBw;
    persistent congestion => cwnd = minCongestionWindow.
```

Для no-loss, stable RTT, non-app-limited upload нельзя резко падать до `MinWindowPackets * MSS` только из-за одного low sample.

---

## 3. Порядок работ

Работай строго в таком порядке:

1. Добавить диагностику и поля в `RateSample`.
2. Исправить delivery-rate sampler.
3. Добавить защиту от self-limited samples.
4. Разделить `canReduceWindow` на отдельные predicates.
5. Исправить state transitions: `ProbeDown`, двойной вызов `shouldEnterProbeDown`, round timer.
6. Добавить pacing floor, связанный с `minCongestionWindow / srtt`.
7. Добавить regression tests.
8. Прогнать `gofmt` и тесты.

Не начинай с изменения дефолтных gain values. Сначала исправь логику, иначе тюнинг будет маскировать баги.

---

## 4. Шаг 1 — расширить диагностику

### 4.1. Расширить `RateSample`

Файл: `internal/congestion/interface.go`

Найди `type RateSample struct` и добавь поля:

```go
type RateSample struct {
    DeliveryRate protocol.ByteCount // bytes/sec
    AckedBytes   protocol.ByteCount
    LostBytes    protocol.ByteCount

    // Cumulative delivered bytes at ACK time.
    DeliveredBytes protocol.ByteCount

    // Delta used to calculate DeliveryRate.
    DeliveredDelta protocol.ByteCount

    PriorInFlight protocol.ByteCount
    Interval      time.Duration

    // Diagnostic breakdown of Interval.
    AckElapsed  time.Duration
    SendElapsed time.Duration

    RTT        time.Duration
    AppLimited bool
    IsValid    bool
}
```

Если часть полей уже есть — не дублируй, только добавь отсутствующие.

### 4.2. Хранить last sample и reasons в `adaptiveBDPSender`

Файл: `internal/congestion/adaptive_bdp_sender.go`

В `adaptiveBDPSender` добавь поля:

```go
lastRateSample RateSample
lastPriorInFlight protocol.ByteCount
lastTargetCwnd protocol.ByteCount
lastBDP protocol.ByteCount
lastQueueDelay time.Duration
lastQueueTarget time.Duration
lastPacingGain float64
lastCwndGain float64

lastStateChangeReason string
lastCwndChangeReason string
lastBWChangeReason string

lastRoundStartTime monotime.Time
```

### 4.3. Добавить human-readable state

Добавь helper:

```go
func (s adaptiveBDPState) String() string {
    switch s {
    case adaptiveBDPStartup:
        return "Startup"
    case adaptiveBDPDrain:
        return "Drain"
    case adaptiveBDPProbeBW:
        return "ProbeBW"
    case adaptiveBDPProbeDown:
        return "ProbeDown"
    case adaptiveBDPProbeRTT:
        return "ProbeRTT"
    default:
        return "Unknown"
    }
}
```

### 4.4. Расширить debug info

Если в проекте уже есть `SendAlgorithmWithDebugInfos`, не ломай существующий интерфейс. Расширь существующую debug struct, либо добавь отдельный метод, если это проще для внутренней диагностики.

Минимальный набор полей, которые должны попадать в лог/метрики:

```text
adaptive_bdp_state
cwnd_bytes
target_cwnd_bytes
min_cwnd_bytes
max_cwnd_bytes
bdp_bytes
bytes_in_flight
prior_in_flight
bw_bytes_per_sec
max_bw_bytes_per_sec
short_bw_bytes_per_sec
pacing_rate_bytes_per_sec
last_delivery_rate_bytes_per_sec
last_delivered_delta
last_sample_interval_ms
last_sample_ack_elapsed_ms
last_sample_send_elapsed_ms
last_sample_app_limited
last_sample_valid
min_rtt_ms
smoothed_rtt_ms
queue_delay_ms
queue_target_ms
round_count
round_start
queue_high_rounds
downshift_rounds
full_bw_reached
probe_up_active
last_state_change_reason
last_cwnd_change_reason
last_bw_change_reason
```

Если pacer budget уже можно получить, добавь ещё:

```text
pacer_budget_bytes
time_until_send_ms
has_pacing_budget
```

### 4.5. Логировать важные события

Добавь debug лог или trace hook на события:

```text
state change
bw changed by >10%
shortBw set / cleared / raised
cwnd decreased
pacing_rate changed by >10%
invalid / suspicious rate sample
ProbeDown enter / exit
persistent congestion
```

Каждое событие должно содержать `reason`.

Примеры reasons:

```text
startup_full_bw_reached
queue_delay_persistent
bandwidth_downshift_pipe_filled
loss_target_exceeded
emergency_loss
ecn_congestion
probe_down_drained
probe_interval_probe_up
persistent_congestion
sampler_suspicious_send_elapsed
```

---

## 5. Шаг 2 — исправить delivery-rate sampler

Файл: `internal/ackhandler/sent_packet_handler.go`

### 5.1. При отправке из idle обновлять `deliveredTime`

Текущий код выставляет `h.firstSentTime = t`, но не должен оставлять `h.deliveredTime` zero/stale.

Исправь блок отправки ack-eliciting packet примерно так:

```go
if isAckEliciting {
    if h.bytesInFlight == 0 {
        h.firstSentTime = t
        h.deliveredTime = t
    }

    p.DeliveredBytes = h.deliveredBytes
    p.DeliveredTime = h.deliveredTime
    p.FirstSentTime = h.firstSentTime
    p.IsAppLimited = h.isSampleAppLimited(t)
    p.TxInFlight = h.bytesInFlight
}
```

Важно: если меняешь порядок, проверь, что первый packet после idle получает `DeliveredTime=t` и `FirstSentTime=t`.

### 5.2. При ACK обновлять `h.firstSentTime` на newest ACKed send time

В ACK loop, где обрабатываются `ackedPackets`, добавь tracking:

```go
var newestAckedSendTime monotime.Time
var sawAckedInFlight bool
```

Внутри `if p.includedInBytesInFlight { ... }` после `h.deliveredBytes += p.Length` и `h.deliveredTime = rcvTime`:

```go
if !sawAckedInFlight || p.SendTime.After(newestAckedSendTime) {
    newestAckedSendTime = p.SendTime
    sawAckedInFlight = true
}
```

После loop, до очистки `ackedPackets`:

```go
if sawAckedInFlight {
    h.firstSentTime = newestAckedSendTime
}
```

Смысл: следующий rate sample должен считать `sendElapsed` от send time newest delivered packet, а не от старта старого flight.

### 5.3. Заполнить diagnostic fields в `makeRateSample`

В `makeRateSample` после вычисления `delivered`, `ackElapsed`, `sendElapsed`, `interval`:

```go
sample.DeliveredDelta = delivered
sample.AckElapsed = ackElapsed
sample.SendElapsed = sendElapsed
sample.Interval = interval
```

Также оставь:

```go
sample.DeliveredBytes = h.deliveredBytes
sample.PriorInFlight = priorInFlight
sample.RTT = h.rttStats.SmoothedRTT()
sample.AppLimited = p.IsAppLimited
```

### 5.4. Отбраковывать явно подозрительные samples

Добавь soft validation, но не делай её слишком агрессивной.

Пример:

```go
func isSuspiciousRateSample(sample congestion.RateSample) bool {
    if !sample.IsValid {
        return true
    }
    if sample.Interval <= 0 || sample.DeliveredDelta <= 0 {
        return true
    }
    // Не invalid-ить автоматически только потому что sendElapsed > ackElapsed:
    // при ACK aggregation это может быть нормально.
    // Но если sendElapsed стал секундным при стабильном RTT, нужно логировать.
    return false
}
```

В `makeRateSample` не надо выкидывать все samples, где `sendElapsed > ackElapsed`. Лучше сначала исправить `firstSentTime`, добавить диагностику и регрессионный тест.

### 5.5. Тест для sampler

Файл: `internal/ackhandler/sent_packet_handler_test.go`

Добавь тест:

```text
TestRateSamplerDoesNotUseStaleFirstSentTimeDuringContinuousUpload
```

Сценарий:

```text
1. Инициализировать sentPacketHandler с AdaptiveBDP mock congestion.
2. Симулировать continuous send: bytesInFlight ни разу не становится 0.
3. ACKать packets каждые ~150ms, как при стабильном RTT.
4. Продолжать 30-60 секунд виртуального времени.
5. Проверить, что sample.SendElapsed не растёт до десятков секунд.
6. Проверить, что delivery_rate остаётся около ожидаемого rate, а не падает обратно к нулю.
```

Acceptance:

```text
sample.SendElapsed <= 2 * RTT или разумный aggregation interval;
sample.DeliveryRate не падает из-за возраста connection flight;
h.firstSentTime обновляется после ACK newest in-flight packet.
```

---

## 6. Шаг 3 — не снижать bandwidth от self-limited samples

Файл: `internal/congestion/adaptive_bdp_sender.go`

### 6.1. Добавить predicate `isPipeFilled`

Добавь helper:

```go
func (s *adaptiveBDPSender) isPipeFilled(priorInFlight protocol.ByteCount) bool {
    if priorInFlight == 0 {
        return false
    }

    pipe := s.bdp()
    if pipe <= 0 {
        pipe = min(s.congestionWindow, s.targetCwnd())
    }
    if pipe <= 0 {
        return false
    }

    threshold := protocol.ByteCount(float64(pipe) * 0.75)
    threshold = max(threshold, 4*s.maxDatagramSize)

    // Tolerance for ack clock and packetization.
    return priorInFlight+2*s.maxDatagramSize >= threshold
}
```

Почему BDP, а не только cwnd: в вашем логе `bytes_in_flight ~= 500KB`, `cwnd ~= 725KB`, fill по cwnd около 69%, но при `30 Mbps / 150ms` BDP около `562KB`, то есть pipe был почти заполнен. Для bandwidth decision важнее BDP, чем текущий cwnd.

### 6.2. Добавить predicate `canUseSampleForDownshift`

```go
func (s *adaptiveBDPSender) canUseSampleForDownshift(sample RateSample, priorInFlight protocol.ByteCount) bool {
    if !sample.IsValid || sample.DeliveryRate == 0 {
        return false
    }
    if sample.AppLimited {
        return false
    }
    if !s.isPipeFilled(priorInFlight) {
        return false
    }
    return true
}
```

### 6.3. Переписать `updateBandwidth`

Требуемое поведение:

```text
- invalid sample не меняет bw вниз;
- app-limited sample может только поднять maxBw, если sampleBW > maxBw;
- non-app-limited sample может поднять maxBw;
- shortBw снижается только после N подряд низких, non-app-limited, pipe-filled samples;
- если pipe не заполнен, низкий sample не является доказательством падения bottleneck;
- shortBw должен уметь восстанавливаться вверх при хороших samples;
- activeBW = min(maxBw, shortBw) только если shortBw > 0;
- если shortBw recovered close to maxBw, можно clear shortBw.
```

Скелет:

```go
func (s *adaptiveBDPSender) updateBandwidth(sample RateSample, priorInFlight protocol.ByteCount) {
    s.lastRateSample = sample
    s.lastPriorInFlight = priorInFlight

    if !sample.IsValid || sample.DeliveryRate == 0 {
        if s.bw == 0 {
            s.bootstrapBandwidth()
            s.lastBWChangeReason = "bootstrap_invalid_sample"
        }
        return
    }

    sampleBW := uint64(sample.DeliveryRate)
    prevMaxBw := s.maxBw
    prevShortBw := s.shortBw
    prevBW := s.bw

    s.updateBandwidthCompetition(sample, sampleBW, prevMaxBw)

    if sample.AppLimited {
        if sampleBW > s.maxBw {
            s.bwFilter.Update(s.roundCount, sampleBW)
            s.maxBw = max(s.maxBw, s.bwFilter.Max(s.roundCount))
            s.lastBWChangeReason = "app_limited_higher_sample"
        }
    } else {
        if s.maxBw == 0 || sampleBW >= s.maxBw {
            s.bwFilter.Update(s.roundCount, sampleBW)
            s.maxBw = max(s.maxBw, s.bwFilter.Max(s.roundCount))
            s.lastBWChangeReason = "non_app_limited_bw_growth"
        }
    }

    activeBW := s.maxBw
    if activeBW == 0 {
        activeBW = sampleBW
    }

    if s.canUseSampleForDownshift(sample, priorInFlight) && activeBW > 0 && float64(sampleBW) < float64(activeBW)*s.downshiftRatio() {
        s.downshiftRounds++
        if s.downshiftRounds >= s.downshiftRoundsTarget() {
            newShort := max(sampleBW, s.minimumObservableBandwidth())
            if s.shortBw == 0 {
                s.shortBw = newShort
            } else {
                s.shortBw = min(s.shortBw, newShort)
            }
            s.enterStateWithReason(adaptiveBDPProbeDown, s.clock.Now(), "bandwidth_downshift_pipe_filled")
            s.lastBWChangeReason = "short_bw_downshift_pipe_filled"
        }
    } else if !sample.AppLimited {
        s.downshiftRounds = 0
        if s.shortBw > 0 && sampleBW > s.shortBw {
            s.shortBw = min(sampleBW, max(s.maxBw, sampleBW))
            s.lastBWChangeReason = "short_bw_recovery"
        }
        if s.shortBw > 0 && s.maxBw > 0 && float64(s.shortBw) >= float64(s.maxBw)*0.95 {
            s.shortBw = 0
            s.lastBWChangeReason = "short_bw_cleared_recovered"
        }
    }

    activeBW = s.maxBw
    if activeBW == 0 {
        activeBW = sampleBW
    }
    if s.shortBw > 0 {
        activeBW = min(activeBW, s.shortBw)
    }

    s.bw = max(1, activeBW)

    if prevBW != s.bw || prevMaxBw != s.maxBw || prevShortBw != s.shortBw {
        // Keep reason already set by branch above.
        if s.lastBWChangeReason == "" {
            s.lastBWChangeReason = "bandwidth_update"
        }
    }
}
```

Добавь helper:

```go
func (s *adaptiveBDPSender) minimumObservableBandwidth() uint64 {
    srtt := s.rttStats.SmoothedRTT()
    if srtt <= 0 {
        srtt = s.minRTT
    }
    if srtt <= 0 {
        srtt = 100 * time.Millisecond
    }
    return max(1, uint64(float64(s.minCongestionWindow)/srtt.Seconds()))
}
```

Этот floor не должен мешать persistent congestion: persistent congestion отдельно сбрасывает `cwnd` и `bw`.

---

## 7. Шаг 4 — разделить `canReduceWindow`

Удалять `canReduceWindow` сразу необязательно, но нельзя использовать его как универсальный gate.

Добавь predicates:

```go
func (s *adaptiveBDPSender) hasQueuePressure() bool {
    return s.queueDelay() > s.queueTarget()
}

func (s *adaptiveBDPSender) hasPersistentQueuePressure() bool {
    return s.queueHighRounds >= s.queuePersistentRounds()
}

func (s *adaptiveBDPSender) shouldReduceCwndForQueue(priorInFlight protocol.ByteCount) bool {
    return s.hasQueuePressure() && s.isPipeFilled(priorInFlight)
}

func (s *adaptiveBDPSender) shouldReduceCwndTowardTarget(priorInFlight protocol.ByteCount, target protocol.ByteCount) bool {
    if s.state == adaptiveBDPProbeDown {
        return true
    }
    if priorInFlight > target && s.hasQueuePressure() {
        return true
    }
    return false
}
```

### 7.1. Loss/ECN не должны зависеть от queue gate

Перепиши `OnCongestionEvent`:

```go
func (s *adaptiveBDPSender) OnCongestionEvent(_ protocol.PacketNumber, lostBytes, priorInFlight protocol.ByteCount) {
    s.connStats.PacketsLost.Add(1)
    s.connStats.BytesLost.Add(uint64(lostBytes))
    s.lostBytesThisRound += lostBytes

    lossRate := s.lossRate()
    if lossRate <= 0 {
        return
    }

    if !s.canLossCutbackThisRound() {
        return
    }

    // If bandwidth is clearly healthy and loss is tiny, allow existing protection,
    // but emergency loss must never be ignored.
    emergency := lossRate > s.emergencyLossThreshold()
    if !emergency && lossRate <= s.lossTarget() {
        return
    }
    if !emergency && s.bandwidthOutweighsLoss() {
        return
    }

    now := s.clock.Now()
    s.enterStateWithReason(adaptiveBDPProbeDown, now, "loss_target_exceeded")

    if emergency && s.canEmergencyCutbackThisRound() {
        s.congestionWindow = max(s.minCongestionWindow, protocol.ByteCount(float64(s.congestionWindow)*0.70))
        s.markEmergencyCutbackRound()
        s.lastCwndChangeReason = "emergency_loss_cutback"
    }

    if s.bw > 0 {
        s.shortBw = max(1, uint64(float64(s.bw)*s.probeDownGain()))
        s.bw = min(s.bw, s.shortBw)
        s.lastBWChangeReason = "loss_probe_down_bw_cutback"
    }

    s.updatePacingRate()
    s.reduceCwndTowardTarget(priorInFlight, true /* lossBased */)
    s.markLossCutbackRound()
}
```

Перепиши `OnECNCongestionEvent` аналогично: ECN должен вести в `ProbeDown` и снижать `shortBw/pacing_rate`, не требуя `queueDelay > queueTarget && priorInFlight < cwnd`.

---

## 8. Шаг 5 — безопасное уменьшение `cwnd`

Текущий `setCwndFromTarget` может сразу сделать:

```go
s.congestionWindow = max(target, s.minCongestionWindow)
```

Если `target` схлопнулся из-за плохого `bw`, это мгновенно отправит `cwnd` в минимум.

Заменить на controlled reduction.

### 8.1. Добавить `reduceCwndTowardTarget`

```go
func (s *adaptiveBDPSender) reduceCwndTowardTarget(priorInFlight protocol.ByteCount, lossBased bool) {
    target := s.targetCwnd()
    old := s.congestionWindow

    if old <= target {
        return
    }

    if lossBased {
        s.congestionWindow = max(target, s.minCongestionWindow)
        s.lastCwndChangeReason = "loss_based_target_cutback"
        return
    }

    // No-loss reduction should be gradual. This prevents collapse from one bad sample.
    maxNoLossCut := protocol.ByteCount(float64(old) * 0.85)
    floor := max(target, maxNoLossCut)

    // If queue pressure is low, don't reduce aggressively.
    if !s.hasQueuePressure() {
        floor = max(floor, protocol.ByteCount(float64(old)*0.95))
    }

    s.congestionWindow = clampCwnd(floor, s.minCongestionWindow, s.maxCongestionWindow)
    if s.congestionWindow < old {
        s.lastCwndChangeReason = "gradual_no_loss_target_cutback"
    }
}
```

### 8.2. Обновить `setCwndFromTarget`

```go
func (s *adaptiveBDPSender) setCwndFromTarget(ackedBytes, priorInFlight protocol.ByteCount) {
    target := s.targetCwnd()
    old := s.congestionWindow

    s.lastTargetCwnd = target
    s.lastBDP = s.bdp()
    s.lastQueueDelay = s.queueDelay()
    s.lastQueueTarget = s.queueTarget()
    s.lastCwndGain = s.cwndGain()
    s.lastPacingGain = s.pacingGain()

    if s.state == adaptiveBDPStartup {
        s.congestionWindow += ackedBytes
        if s.congestionWindow < target {
            maxStep := max(ackedBytes, 2*s.maxDatagramSize)
            s.congestionWindow = min(target, s.congestionWindow+maxStep)
        }
    } else if s.congestionWindow < target {
        s.congestionWindow += min(ackedBytes, target-s.congestionWindow)
        s.lastCwndChangeReason = "ack_growth_toward_target"
    } else if s.shouldReduceCwndTowardTarget(priorInFlight, target) {
        s.reduceCwndTowardTarget(priorInFlight, false /* lossBased */)
    }

    s.congestionWindow = clampCwnd(s.congestionWindow, s.minCongestionWindow, s.maxCongestionWindow)

    if s.congestionWindow < old && s.lastCwndChangeReason == "" {
        s.lastCwndChangeReason = "cwnd_reduced"
    }
}
```

---

## 9. Шаг 6 — исправить `ProbeDown` и state transitions

### 9.1. Не вызывать `shouldEnterProbeDown` дважды

В `OnPacketAckedWithRateSample` замени:

```go
if s.shouldEnterProbeDown(sample, priorInFlight) {
    s.enterState(adaptiveBDPProbeDown, eventTime)
}

if s.state == adaptiveBDPStartup && (s.fullBwReached || s.shouldEnterProbeDown(sample, priorInFlight)) {
    s.enterState(adaptiveBDPDrain, eventTime)
}
```

на:

```go
enterProbeDown := s.shouldEnterProbeDown(sample, priorInFlight)

if enterProbeDown {
    s.enterStateWithReason(adaptiveBDPProbeDown, eventTime, "probe_down_decision")
}

if s.state == adaptiveBDPStartup && s.fullBwReached {
    s.enterStateWithReason(adaptiveBDPDrain, eventTime, "startup_full_bw_reached")
}
```

Если очередь/падение bandwidth требует `ProbeDown`, не надо сначала заходить в `Drain`. `Drain` нужен для выхода из Startup при full bandwidth reached, а `ProbeDown` — для congestion/queue/downshift.

### 9.2. Исправить выход из `ProbeDown`

Заменить:

```go
if priorInFlight <= s.bdp() || s.queueDelay() <= s.queueTarget()/2 || eventTime.Sub(s.lastStateChange) >= minDrain {
    s.enterState(adaptiveBDPProbeBW, eventTime)
}
```

на:

```go
drained := priorInFlight <= s.bdp() || s.queueDelay() <= s.queueTarget()/2
spentMinDrain := eventTime.Sub(s.lastStateChange) >= minDrain

if spentMinDrain && drained {
    s.enterStateWithReason(adaptiveBDPProbeBW, eventTime, "probe_down_drained")
}
```

То есть один RTT — минимальное время в drain, но не самостоятельная причина выхода.

### 9.3. Добавить `enterStateWithReason`

```go
func (s *adaptiveBDPSender) enterStateWithReason(st adaptiveBDPState, now monotime.Time, reason string) {
    if s.state == st {
        if reason != "" {
            s.lastStateChangeReason = reason
        }
        return
    }
    s.state = st
    s.lastStateChange = now
    s.lastStateChangeReason = reason

    if st == adaptiveBDPProbeBW {
        s.queueHighRounds = 0
        s.downshiftRounds = 0
    }
}

func (s *adaptiveBDPSender) enterState(st adaptiveBDPState, now monotime.Time) {
    s.enterStateWithReason(st, now, "")
}
```

### 9.4. Разделить round timer и state timer

В конструкторе `NewAdaptiveBDPSender`:

```go
now := clock.Now()
s.lastStateChange = now
s.lastRoundStartTime = now
s.lastProbeTime = now
```

В `updateRound` заменить fallback на `lastRoundStartTime`:

```go
if !s.roundStart {
    if s.lastRoundStartTime.IsZero() || now.Sub(s.lastRoundStartTime) >= s.minRTT {
        s.roundStart = true
    }
}

if s.roundStart {
    s.roundCount++
    s.lastRoundStartTime = now
    ...
}
```

Не используй `lastStateChange` как proxy для round start.

---

## 10. Шаг 7 — pacing floor

Сейчас `updatePacingRate` может упасть до `1024 bytes/sec`. При `tx_write_pending=20480` это объясняет записи по несколько секунд.

Сделай нижний pacing floor согласованным с min cwnd:

```go
func (s *adaptiveBDPSender) minimumPacingRate() uint64 {
    srtt := s.rttStats.SmoothedRTT()
    if srtt <= 0 {
        srtt = s.minRTT
    }
    if srtt <= 0 {
        srtt = 100 * time.Millisecond
    }

    // At least drain/fill min cwnd over roughly one RTT.
    floor := uint64(float64(s.minCongestionWindow) / srtt.Seconds())
    return max(uint64(1024), floor)
}
```

В `updatePacingRate`:

```go
rate := float64(s.bw) * s.pacingGain() * (1 - s.pacingMargin())

if maxProbe := s.cfg.MaxProbeRateBps; maxProbe > 0 {
    rate = min(rate, float64(maxProbe)/8.0)
}

minRate := s.minimumPacingRate()
s.pacingRateBytesPerSecond = max(uint64(rate), minRate)
```

Важно: это не отменяет congestion control. Если ACK не приходит, `bytesInFlight` заполнит `cwnd`, и отправка остановится. Но если `MinWindowPackets=32`, pacer не должен превращать это в 1 KB/s.

---

## 11. Шаг 8 — улучшить `shouldEnterProbeDown`

Текущая функция мутирует `queueHighRounds` и сразу смотрит loss/downshift через `canReduceWindow`.

Перепиши структуру:

```go
func (s *adaptiveBDPSender) shouldEnterProbeDown(sample RateSample, priorInFlight protocol.ByteCount) bool {
    if s.hasQueuePressure() && s.isPipeFilled(priorInFlight) {
        s.queueHighRounds++
    } else {
        s.queueHighRounds = 0
    }

    if s.queueHighRounds >= s.queuePersistentRounds() {
        s.lastStateChangeReason = "queue_delay_persistent"
        return true
    }

    if s.lossRate() > s.lossTarget() && !s.bandwidthOutweighsLoss() {
        s.lastStateChangeReason = "loss_target_exceeded"
        return true
    }

    if s.canUseSampleForDownshift(sample, priorInFlight) && s.bw > 0 && float64(sample.DeliveryRate) < float64(s.bw)*s.downshiftRatio() {
        // Do not enter immediately here unless downshiftRounds reached target.
        // updateBandwidth owns downshiftRounds and shortBw.
        return s.downshiftRounds >= s.downshiftRoundsTarget()
    }

    return false
}
```

Не увеличивай `downshiftRounds` здесь, если это уже делает `updateBandwidth`. Один counter должен мутировать в одном месте.

---

## 12. Шаг 9 — StartupTargetRateBps не должен быть ложной гарантией

Оставь смысл `StartupTargetRateBps` таким:

```text
StartupTargetRateBps помогает рассчитать минимальный startup pacing gain,
но не является постоянным floor bandwidth.
```

Однако добавь защиту в Startup:

- пока нет loss/ECN/persistent queue;
- sample valid или bootstrap;
- sender non-app-limited;
- elapsed from startup < `StartupTargetDuration`;

не срезать `cwnd` ниже `initialWindow`, и не снижать `pacing_rate` ниже `initialWindow / srtt`.

Минимальная защита:

```go
func (s *adaptiveBDPSender) inProtectedStartup(now monotime.Time) bool {
    if s.state != adaptiveBDPStartup {
        return false
    }
    d := s.cfg.StartupTargetDuration
    if d <= 0 {
        d = 5 * time.Second
    }
    return !s.lastStateChange.IsZero() && now.Sub(s.lastStateChange) <= d
}
```

В no-loss startup не делать no-loss cutback ниже `initialWindow`:

```go
if s.inProtectedStartup(s.clock.Now()) && !lossBased {
    s.congestionWindow = max(s.congestionWindow, s.initialWindow)
}
```

Не делай долгосрочный floor на `StartupTargetRateBps`, иначе алгоритм не сможет качественно ужаться при реальном падении канала.

---

## 13. Регрессионные тесты

### 13.1. `internal/ackhandler/sent_packet_handler_test.go`

Добавить:

```text
TestRateSamplerDoesNotUseStaleFirstSentTimeDuringContinuousUpload
TestRateSamplerSetsDeliveredTimeWhenSendingFromIdle
TestRateSamplerExportsIntervalBreakdown
```

Acceptance:

```text
continuous upload 30-60s, RTT=150ms:
  SendElapsed не растёт пропорционально total connection time;
  DeliveryRate остаётся около simulated bottleneck;
  DeliveredDelta и Interval заполнены;
  AppLimited выставляется как раньше, без регрессий.
```

### 13.2. `internal/congestion/adaptive_bdp_sender_test.go`

Добавить / обновить:

```text
TestAdaptiveBDPNoLossDoesNotCollapseToMinWindow
```

Сценарий:

```go
cfg := CwndTuningConfig{
    Enable: true,
    Algorithm: CongestionControlAdaptiveBDP,
    InitialWindowPackets: 256,
    MinWindowPackets: 32,
    StartupTargetRateBps: 30_000_000,
    StartupTargetDuration: 5 * time.Second,
}
```

Model:

```text
RTT=150ms
bottleneck=30Mbps
no loss
not app-limited
run >= 15s virtual time
```

Assert:

```text
cwnd never equals 40960 after healthy startup;
pacing_rate >= 0.8 * 30Mbps after startup;
target_cwnd >= 0.8 * BDP * cruiseCwndGain;
state is not stuck in ProbeDown;
bytes_in_flight model not stuck <10% cwnd while non-app-limited.
```

Добавить:

```text
TestAdaptiveBDPIgnoresSelfLimitedLowSampleForDownshift
```

Сценарий:

```text
bw estimate already 30Mbps;
priorInFlight tiny, e.g. 4KB;
sample non-app-limited but delivery_rate very low;
```

Assert:

```text
shortBw unchanged;
bw unchanged;
downshiftRounds == 0;
no ProbeDown solely from this sample.
```

Добавить:

```text
TestAdaptiveBDPDownshiftsOnPipeFilledBandwidthDrop
```

Сценарий:

```text
bw estimate 30Mbps;
priorInFlight >= 0.75 * BDP;
2-3 consecutive non-app-limited samples around 10Mbps;
```

Assert:

```text
shortBw set near 10Mbps;
bw becomes min(maxBw, shortBw);
pacing_rate drops within 2-3 RTT;
state enters ProbeDown.
```

Добавить:

```text
TestAdaptiveBDPProbeDownRequiresDrainAndMinRTT
```

Assert:

```text
ProbeDown does not exit only because minRTT elapsed;
ProbeDown exits only after minRTT elapsed AND (priorInFlight <= BDP OR queueDelay <= queueTarget/2).
```

Добавить:

```text
TestAdaptiveBDPLossCutbackDoesNotDependOnQueueGate
TestAdaptiveBDPECNDoesNotDependOnQueueGate
```

Assert:

```text
loss/ECN above threshold enters ProbeDown and reduces pacing_rate/cwnd target even when old canReduceWindow would have returned false.
```

Добавить:

```text
TestAdaptiveBDPNoLossCwndReductionIsGradual
```

Assert:

```text
without loss/ECN/persistent congestion, one ACK/sample cannot reduce cwnd by more than configured no-loss cut ratio, e.g. 15% per round.
```

Добавить:

```text
TestAdaptiveBDPPacingFloorUsesMinCwndOverRTT
```

Scenario:

```text
MinWindowPackets=32, MSS=1280, srtt=150ms.
minimumPacingRate ~= 40960 / 0.150 ~= 273066 bytes/sec.
```

Assert:

```text
pacingRateBytesPerSecond >= minimumPacingRate;
pacingRateBytesPerSecond is not 1024 bytes/sec after non-persistent no-loss state.
```

### 13.3. Optional integration/model test

Добавить lightweight network model test, даже если он не идеально физический:

```text
TestAdaptiveBDPThirtyMbps150msNoLossModel
```

Model:

```text
capacity = 30 Mbps
baseRTT = 150ms
MSS = 1280
app always has data
ACK every RTT for delivered bytes = min(inflight, capacity * RTT)
no loss
run 20 seconds
```

Acceptance:

```text
by 5s:
  pacing_rate >= 0.8 * 30Mbps
  cwnd >= 0.8 * BDP * cruiseCwndGain

from 5s to 20s:
  cwnd never collapses to MinWindowPackets*MSS
  pacing_rate never collapses below 0.5 * 30Mbps
```

---

## 14. Operational test config после фикса

Для вашего текущего теста оставь близко к исходному, чтобы проверить именно bugfix:

```go
CwndTuning: quic.CwndTuning{
    Enable: true,
    Algorithm: quic.CongestionControlAdaptiveBDP,

    InitialWindowPackets: 256,
    MinWindowPackets:     32,
    MaxWindowPackets:     2000,

    StartupTargetRateBps: 30_000_000,
    StartupTargetDuration: 5 * time.Second,

    StartupPacingGain: 2.0,
    StartupCwndGain:   2.0,

    CruisePacingGain: 1.0,
    CruiseCwndGain:   1.5,

    ProbeUpGain:   1.15,
    ProbeDownGain: 0.90,

    QueueTarget:           30 * time.Millisecond,
    QueuePersistentRounds: 3,

    DownshiftRatio:  0.75,
    DownshiftRounds: 3,

    PacingMargin: 0.01,
}
```

Для диагностики можно временно поставить `MinWindowPackets: 256` или `512`, но это не должно быть финальным исправлением. Финальное исправление — sampler + self-limited guard + controlled cwnd reduction.

---

## 15. Acceptance criteria для всей задачи

Код считается исправленным, если выполняется всё:

```text
1. No-loss stable upload, RTT 148-150ms, target 30Mbps:
   - cwnd не падает до 40960 после начального здорового участка;
   - pacing_rate не падает до ~1024 bytes/sec;
   - stream.Write 20KB не висит по 3-8 секунд из-за QUIC pacer/cwnd;
   - tx_app_limited=false и tx_write_pending>0 не сочетаются долго с bytes_in_flight <10% cwnd.

2. Continuous upload >60s:
   - delivery_rate не деградирует из-за растущего sendElapsed;
   - SendElapsed в RateSample остаётся bounded delivery interval, а не возраст всего flight.

3. App-limited / sender-limited samples:
   - app-limited low samples не снижают maxBw/shortBw;
   - sender-limited low samples не вызывают downshift;
   - high app-limited sample может поднять maxBw.

4. Реальное падение bottleneck:
   - при pipe-filled samples ниже estimate несколько RTT подряд shortBw снижается;
   - pacing_rate падает за 2-3 RTT;
   - cwnd поджимается постепенно, без долгой очереди.

5. Loss/ECN:
   - loss/ECN выше threshold ведут к ProbeDown независимо от старого canReduceWindow;
   - emergency loss режет cwnd сильнее;
   - persistent congestion всегда ставит cwnd=minCongestionWindow.

6. ProbeDown:
   - не выходит только потому, что прошёл один RTT;
   - выходит после minRTT и фактического drain.

7. Tests:
   - go test ./internal/congestion ./internal/ackhandler .
   - затем go test ./...
```

---

## 16. Что не делать

Не делай эти изменения:

```text
- Не лечить проблему простым увеличением MinWindowPackets.
- Не делать StartupTargetRateBps постоянным bandwidth floor.
- Не ставить GetCongestionWindow() = max(cwnd, bdpTarget).
- Не снижать bw/shortBw от app-limited или sender-limited samples.
- Не игнорировать loss/ECN из-за queue-delay gate.
- Не выходить из ProbeDown только по таймеру.
- Не менять одновременно все gains до исправления sampler и state logic.
```

---

## 17. Краткая карта файлов

```text
internal/ackhandler/sent_packet_handler.go
  - исправить send/ack delivery-rate sampler;
  - обновлять firstSentTime при ACK newest delivered packet;
  - добавить RateSample diagnostics.

internal/ackhandler/packet.go
  - убедиться, что packet хранит DeliveredBytes, DeliveredTime, FirstSentTime, IsAppLimited, TxInFlight.

internal/congestion/interface.go
  - расширить RateSample диагностическими полями.

internal/congestion/adaptive_bdp_sender.go
  - добавить debug state/reasons;
  - добавить isPipeFilled/canUseSampleForDownshift;
  - переписать updateBandwidth;
  - разделить canReduceWindow;
  - исправить loss/ECN reaction;
  - controlled cwnd reduction;
  - исправить ProbeDown exit;
  - убрать двойной shouldEnterProbeDown;
  - добавить lastRoundStartTime;
  - добавить minimumPacingRate.

internal/congestion/pacer.go
  - менять только если нужно экспортировать budget/time-until-send в debug.

internal/congestion/adaptive_bdp_sender_test.go
  - добавить regression/model tests.

internal/ackhandler/sent_packet_handler_test.go
  - добавить rate sampler tests.
```

---

## 18. Финальная проверка в логах

После патча в реальном клиентском логе при вашем сценарии нужно увидеть примерно:

```text
state=Startup -> Drain -> ProbeBW
bw_bytes_per_sec ~= 3_000_000..4_000_000 for 30Mbps path
pacing_rate_bytes_per_sec ~= 3_700_000 или около того во время startup/probe,
  затем около bw * cruise_gain * margin
bdp_bytes ~= 550_000..600_000
cwnd_bytes ~= 800_000..1_200_000 в зависимости от cwnd_gain
queue_delay_ms стабильно около target или ниже
last_sample_send_elapsed_ms не растёт до секунд при стабильном RTT
shortBw=0 в stable no-loss case
last_bw_change_reason не показывает short_bw_downshift_pipe_filled без реального падения delivery rate
last_cwnd_change_reason не показывает collapse до min без loss/persistent congestion
```

Если после фикса снова появится:

```text
cwnd_bytes=40960
pacing_rate_bytes_per_sec очень маленький
last_sample_send_elapsed_ms сильно больше RTT
last_bw_change_reason=short_bw_downshift_...
```

значит sampler всё ещё использует stale time base или downshift всё ещё принимает self-limited samples.
