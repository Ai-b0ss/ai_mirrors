metadata
title: Ai Mirrors
emoji: 🔥
colorFrom: pink
colorTo: indigo
sdk: docker
app_port: 7860
pinned: false
Mirror Bot
Telegram bot service for Hugging Face Spaces.

Required Space secrets:

BOT_TOKEN
ALLOWED_USERS
Optional secrets depend on enabled integrations, for example OPENAI_API_KEY, HF_TOKEN, HF_DATASET, NOTION_WS_JSON, proxy settings, and search provider keys.

🧬 GPT 5.5 Mirror — личный Telegram-бот-агрегатор ИИ
Личный (однопользовательский) Telegram-бот, дающий доступ ко всем топовым LLM (GPT-5.x, Claude Opus/Sonnet, Gemini Pro/Flash) через единый шлюз с умным авто-роутером, глубоким веб-ресёрчем и оркестратором задач.

Telegram: @gpt67bot · Хостинг: HuggingFace Space i99er/ai_mirrors · Шлюз моделей: api.byesu.com


📑 Содержание
TL;DR
Идентификация проекта
Инфраструктура и топология
Архитектура: 6 слоёв
Слой 1 — транспорт и прокси
Слой 2 — шлюз моделей
Слой 3 — модели, каналы и авто-роутер
Слой 4 — надёжность
Слой 5 — поиск и DeepResearch
Слой 6 — состояние и персистентность
Команды и фичи
/brain — оркестратор «Мегамозг»
Интеграция с Notion
Пул-ротатор воркспейсов
Безопасность и переменные окружения
Зависимости и QA
Развитие
Перенос / деплой
Глоссарий
⚡ TL;DR — суть за минуту
Что это: однопользовательский Telegram-бот-агрегатор ИИ. Один человек, один файл app.py (~5 900 строк). Модель разработки — «вайбкод»: код генерит ИИ (Codex), правки применяются патчами.
Главная идея: один бот = доступ ко всем топ-моделям через единый платный шлюз api.byesu.com, с умным авто-роутером, который сам выбирает модель под задачу и переключается при сбоях.
Две фишки-флагмана:
/research — глубокий веб-ресёрч по 7+ источникам с дедупом, реранком и судьёй.
/brain («Мегамозг») — оркестратор: разбивает задачу на подзадачи, считает смету, спрашивает подтверждение, собирает финальный ответ с кросс-проверкой.
Где живёт: HuggingFace Space i99er/ai_mirrors (бесплатный CPU), состояние бэкапится в HF Dataset. Telegram пробивается через Cloudflare Worker (HF блокирует прямой доступ).
Сделано сверх плана: Авто-роутер 2.0, Поиск 2.0 (Этапы 1–9), /brain v1, интеграция задача: → Notion → агент Fable 5, пул-ротатор кредитов между воркспейсами, реестр из 22+ фиксов код-ревью.
Куда движется: сначала проверка применённых патчей (быстрый путь против «привет → дорогая модель» и память), затем RAG по личному Notion / режим наставника. Совет моделей (/council) отложен как дорогой режим, не приоритет.
🪪 Идентификация проекта
Параметр	Значение
Названия	«GPT 5.5 Mirror» / «ai_mirrors»; внутренний логгер mirror-bot
Telegram	@gpt67bot
HF Space	i99er/ai_mirrors (ранее i99er/tg_bot_claude_and_more)
HF Dataset (бэкап)	i99er/ai-mirrors-state → файл bot_state.json
Главный файл	app.py (~5 929 строк с фиксами, актуальная база app_brain_final.py)
Деплой	HF Spaces, Flask keep-alive на порту 7860, деплой = «Factory reboot»
Модель разработки	«вайбкод»: код генерит ИИ (Codex), человек вставляет патчи; ритуал перед деплоем — py_compile + чистка невидимых символов (U+200B / zero-width) и U+FFFD
🗺️ Инфраструктура и топология
flowchart LR
    U["Пользователь<br>(Telegram)"] --> TG["Telegram API"]
    TG <--> CF["Cloudflare Worker<br>tg-proxy…workers.dev"]
    CF <--> BOT["app.py на HF Space<br>i99er/ai_mirrors"]
    BOT --> GW["Шлюз моделей<br>api.byesu.com"]
    GW --> M1["OpenAI GPT-5.x"]
    GW --> M2["Anthropic Claude"]
    GW --> M3["Google Gemini"]
    BOT --> WEB["Веб-поиск:<br>Tavily/Brave/SearXNG/…"]
    BOT --> ST["Состояние:<br>JSON + HF Dataset"]
    BOT --> PX["Прокси-пулы"]
    BOT -.->|"задача:"| NO["Notion → Fable 5"]
Почему так: HuggingFace Spaces блокирует прямой исходящий доступ к Telegram, а резидентные прокси Telegram режет. Решение — обратный прокси на Cloudflare Worker: бот общается с Worker (TG_API_WORKER), а тот ходит в Telegram (переопределение apihelper.API_URL / apihelper.FILE_URL).

🏗️ Архитектура: 6 слоёв
Весь бот — монолитный app.py, логически делится на 6 слоёв.

Транспорт. Telegram ↔ Cloudflare Worker ↔ long-polling. Прокси-пулы для исходящих запросов к моделям/поиску.
Шлюз моделей. Единая точка api.byesu.com (OpenAI-совместимый): /v1 — OpenAI + Claude, /v1beta — нативный Gemini.
Авто-роутер. classify_task → _capability_score → выбор канала/модели; ModelHealth (circuit breaker); цепочки фоллбэка.
Надёжность. SSE-стриминг, watchdog (HARD_CAP_SECS=300), пул генерации (12 воркеров), ретраи, локи состояния.
Веб-поиск / DeepResearch. 7+ провайдеров → RRF-слияние → SimHash/Jaccard-дедуп → MMR → судья источников → ответ с цитатами.
Состояние. Локальный JSON + бэкап в HF Dataset.
🔌 Слой 1 — транспорт, Telegram и прокси
Cloudflare Worker (обязательный): ENV TG_API_WORKER = адрес воркера. Бот переопределяет apihelper.API_URL и apihelper.FILE_URL, чтобы весь трафик к Telegram шёл через Worker.

import os
import telebot.apihelper as apihelper

apihelper.API_URL  = os.environ["TG_API_WORKER"].rstrip("/") + "/bot{0}/{1}"
apihelper.FILE_URL = os.environ["TG_API_WORKER"].rstrip("/") + "/file/bot{0}/{1}"
Референс Worker (обратный прокси):

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const target = "https://api.telegram.org" + url.pathname + url.search;
    return fetch(target, {
      method: request.method,
      headers: request.headers,
      body: ["GET", "HEAD"].includes(request.method) ? undefined : request.body,
    });
  },
};
Telegram-токен виден в пути запроса внутри Worker — Worker должен быть приватным.

Прокси-пулы (для моделей/поиска, не для Telegram):

Провайдер	Адрес	Ключевые ENV
1024proxy (основной)	us.1024proxy.io:3000 (+ суффикс региона)	PROXY_1024_PASS, PROXY_1024_REGIONS, PROXY_1024_SCHEME, PROXY_1024_TTL
floppydata (фоллбэк)	geo.floppydata.com:10080	PROXY_FALLBACK, PROXY_FALLBACK_JSON
Общие ENV: PROXY_AUTO, PROXY_DIRECT_FALLBACK, PROXY_DIRECT_AFTER, PROXY_PROBE_URL, PROXY_REGION.

Надёжность транспорта: CONNECT_TIMEOUT=30, READ_TIMEOUT=90, RETRY_ON_ERROR=True, num_threads=8. Обёртка _tg_retry — до 5 попыток; на 429 читает retry_after. Выбор прокси по пингу: pick_fastest_proxy → measure_all_proxies → rank_proxies; после PROXY_DIRECT_AFTER=3 неудач — прямое подключение с фоновым reselect_proxy.

🚪 Слой 2 — шлюз моделей
Все модели идут через один OpenAI-совместимый шлюз api.byesu.com:

/v1 — OpenAI и Claude.
/v1beta — нативный Gemini (голос, картинки, мультимодал).
5 ключей у шлюза → 4 канала роутинга. Биллинг-группы byesu (как на скриншоте ключей): KEY_GPT_PLUS (GPT Plus), KEY_GPT_PRO (GPT Pro), KEY_GEMINI (Gemini), KEY_CLAUDE (Claude MAX), KEY_CLAUDE_KIRO (Claude Kiro — дешевле). Но каналов роутинга всего 4 (gemini/gpt_plus/gpt_pro/claude): Claude MAX и Claude Kiro — два ключа ОДНОГО канала claude (claude_api_key() берёт сперва Kiro, при пустом — KEY_CLAUDE). Отдельного канала kiro в коде нет.
Флаг GEMINI_VIA_OPENAI — гонять Gemini через OpenAI-совместимый путь.
Маскировка клиента под официальные CLI: запросы уходят с CLIENT_HEADERS (User-Agent: opencode/1.0) и CLAUDE_CLIENT_HEADERS (anthropic-version: 2023-06-01, набор x-stainless-*).
🧭 Слой 3 — модели, каналы и Авто-роутер 2.0
Каналы и модели
CHANNEL_ORDER = ["gemini", "gpt_plus", "gpt_pro", "claude"] — 4 канала (не 5; Claude Kiro живёт внутри claude как более дешёвый ключ).
DEFAULT_MODEL = "gemini-3.5-flash" (бесплатный движок -c).
AUTO_CHEAP_CHANNELS = {"gpt_plus"}. ⚠️ Баг: в MODEL_CHANNEL дорогие gpt-5.4 и gpt-5.5 тоже отнесены к каналу gpt_plus, поэтому авто-переход считает их «дешёвыми» и может молча эскалировать на них (см. техдолг).
Видимые модели (6): GPT-5.4 mini, GPT-5.5, Gemini 3.1 Pro, Gemini 3.5 Flash, Claude Opus 4.8, Claude Sonnet 4.6. Плюс 10 скрытых (HIDDEN_MODELS: gpt-5.4, gpt-5.3-codex / codex-spark, claude-opus-4-7 / 4-6, claude-haiku-4-5, gemini-2.5-pro / 2.5-flash / 2.5-flash-lite, gemini-3.5-flash-low) — итого 16 моделей в роутере.
Логика роутера
classify_task относит запрос к одному из 15 классов (general_chat, reasoning, coding_simple, coding_complex, code_review, research, creative_writing, summarization, translation, strict_json, multimodal_image, high_stakes_factual, long_context, fast_simple, unknown). Сначала дешёвая эвристика, при уверенности < 0.7 — LLM-классификатор с кешем.
_capability_score / MODEL_CAPS — оценка по 9 осям способностей + здоровье модели + диверсификация провайдеров. Резервный порядок — DEEP_FALLBACK_ORDER; потолок усилия дешёвых — FALLBACK_EFFORT_CAP.
ModelHealth — circuit breaker (помечает «мёртвые» модели, кулдаун; ⚠️ в памяти, теряется при ребуте).
Дедлайны: DEADLINE_CHEAP=15, DEADLINE_MID=40, DEADLINE_TOP=75, DEADLINE_FLASH=35; CHANNEL_DEAD_TTL=60.
Thinking: по умолчанию low, авто-роутер поднимает до high; /effort работает как «пол».
Наблюдаемость: /why, /health, /listmodels.
Цены: канальные множители
Модель / канал	$ за 1М токенов	Трактовка
Gemini Flash	$0.00	Практически бесплатный слой
GPT mini (gpt_plus)	~$0.03	Дешёвый платный fallback
GPT 5.4 (gpt_pro)	~$0.62	Сильный reasoning по умеренной цене
Kiro / Claude opus (via key)	~$0.65	Эмпирика по usage
GPT 5.5 (gpt_pro)	~$1.26	Осознанное применение
Claude Sonnet	~$4.27	Дорогой продвинутый слой
Claude Opus	~$7.34	Самый дорогой
В /brain тарифы задаются по моделям через BRAIN_RATE (in/out за 1М токенов); Gemini-модели считаются как $0.

🛡️ Слой 4 — надёжность
Стриминг: SSE, ответ редактируется по мере генерации; send_html / chunk_text режут под лимит Telegram.
Таймауты/ретраи: connect ~30с, read ~120с; до 4 ретраев на 503; circuit breaker по EMA-латентности. Бесплатные Gemini (-c) не стримятся.
Watchdog: HARD_CAP_SECS=300 — максимум на ответ; STREAM_STALL_SECS=20 — обрыв «зависшего» стрима.
Пул генерации: _GEN_POOL — 12 воркеров (GEN_POOL).
HTTP-таймаут: cand["_http_timeout"] = HARD_CAP_SECS + 30.
Локи: _state_lock (RLock), _chat_lock(chat_id), _PENDING_LOCK.
Отмена: кнопка ⏹ Стоп, словари CANCELS, PENDING_ROUTE, MEDIA_GROUPS.
Принцип-цель: сбой дешёвой/бесплатной модели не должен приводить к эскалации на дорогую (см. техдолг).

🔎 Слой 5 — Поиск 2.0 и DeepResearch (/research)
flowchart LR
    Q["Запрос"] --> AG["Агентная разбивка<br>на под-вопросы"]
    AG --> P["Провайдеры:<br>Tavily/Brave/SearXNG/<br>Wiki/OpenAlex/HN/GitHub"]
    P --> RRF["RRF-слияние"]
    RRF --> DD["SimHash + Jaccard дедуп"]
    DD --> EMB["(опц.) e5-префильтр"]
    EMB --> RR["(опц.) кросс-энкодер"]
    RR --> MMR["MMR-диверсификация"]
    MMR --> J["_judge_sources"]
    J --> A["Ответ с цитатами [N]"]
    A --> V["_verify_answer<br>grounding-проверка"]
Что сделано по этапам (1–8)
Вертикальный роутинг + RRF (k=60), дедуп по canonicalize_url. Источники: Tavily, Brave, SearXNG, OpenAlex, Hacker News, GitHub (за ENV-ключами); DDG — фолбэк, Wikipedia — последний.
Каскад извлечения текста. HTML: trafilatura → readability-lxml → BeautifulSoup → regex. PDF: PyMuPDF → pypdf → pdfplumber. JS/paywall: r.jina.ai. YouTube-транскрипты.
Семантический дедуп + MMR. SimHash-64 (Hamming ≤ 3) или Jaccard ≥ 0.85; MMR (λ = 0.7 fast / 0.6 deep).
Нейро-реранк кросс-энкодером (cross-encoder/mmarco-mMiniLMv2-L12-H384-v1). По умолчанию выключен (RERANK_ENABLED=0) — требует sentence-transformers/torch; без них мягко деградирует.
Семантический префильтр e5-small (intfloat/multilingual-e5-small). По умолчанию выключен (EMBED_ENABLED=0) — те же тяжёлые зависимости.
Умный LLM-судья. При нейро-скорах пропускает LLM-вызов; для свежих запросов остаётся.
Пофактовые цитаты + grounding. Каждое утверждение несёт [N]; verify-pass проверяет опору.
Агентная декомпозиция. Bounded-DAG: 2–3 под-вопроса (AGENTIC_MAX_SUBQ=3).
Текущий статус поисковых ключей
TAVILY_API_KEY — подключён и является основным платным/качественным веб-поиском.
Wikipedia и DuckDuckGo — fallback без ключей.
BRAVE_API_KEY — опциональный апгрейд: код поддерживает Brave, но ключ можно добавить позже. Для хорошего запаса достаточно 3 ключей, с максимумом до 5; multi-key-ротация требует отдельного патча Codex.
SEARXNG_URL / SEARXNG_TOKEN — опциональный self-hosted провайдер. Не блокер: требует отдельной VM (например Oracle Always Free), поэтому пока отложен.
OpenAlex / GitHub — опциональные вертикальные источники, подключаются по необходимости.
Этап 9 — эмпирический тюнинг
Диагностический лог-воронки за SEARCH_DEBUG=1 (префикс [FUNNEL]). Методика — строго A/B по одной ENV-ручке: baseline → меняешь ручку → Factory reboot → прогон эталонного набора (12–15 вопросов).

Ручка	ENV	Сейчас	Диапазон
RRF k	RRF_K	60	40–80
SimHash дистанция	SIMHASH_MAX_DIST	3	2–5
Jaccard порог	JACCARD_THR	0.85	0.80–0.92
MMR λ (fast/deep)	MMR_LAMBDA_FAST/DEEP	0.7 / 0.6	0.5–0.8
MMR top_n (fast/deep)	MMR_TOP_N_FAST/DEEP	24 / 48	16–60
Отбор судьи (fast/deep)	JUDGE_KEEP_FAST/DEEP	16 / 14	8–20
Глубина агента	AGENTIC_MAX_SUBQ	3	2–5
Раунды поиска	RESEARCH_ROUNDS	2	1–3
Окно факт-чекера	VERIFY_CTX_CHARS	20000	12000–22000
Ключевые функции: gather_web_context, deep_research_context, _consolidate_candidates, _dedup_simhash, _mmr_rank, _judge_sources, _agentic_deep_research, _verify_answer, _fetch_url_text_impl.

💾 Слой 6 — состояние и персистентность
Локальный STATE (dict) → JSON; фоновый _state_writer_loop пишет каждые ~2с по флагу _local_dirty.
_hf_backup_loop — бэкап в HF Dataset i99er/ai-mirrors-state (bot_state.json); восстановление по _saved_at.
Флаг _backup_safe: если облако недоступно при старте, бэкап ставится на паузу (чтобы пустое состояние не затёрло хорошее). Локальная запись атомарна (*.tmp → os.replace).
ФС HF Spaces эфемерна — без бэкапа состояние теряется при ребуте.

Структура bot_state.json:

{
  "_saved_at": 1750000000,
  "_backup_safe": true,
  "users": {
    "<user_id>": {
      "model": "gemini-3.5-flash",
      "effort": "low",
      "persona": null,
      "web": false,
      "active_chat": "<chat_id>",
      "hidden_models": [],
      "chats": {
        "<chat_id>": {
          "id": "<chat_id>",
          "title": "Авто-заголовок",
          "created": 1750000000,
          "history": [
            {"role": "user", "content": "...", "ts": 1750000000},
            {"role": "assistant", "content": "...", "ts": 1750000001}
          ]
        }
      },
      "stats": {"msgs": 0, "tokens": 0}
    }
  },
  "brain_pending_<chat_id>": {"plan": "...", "estimate_usd": 0.0}
}
⌨️ Команды и фичи
Команды: /start /model /effort /clear /new /stats /export /regenerate (/regen) /why /health /ping /research /brain /listmodels /whoami.

Уровни усилия (/effort): low / medium / high / xhigh → 4k / 8k / 16k / 24k токенов рассуждений.

Доступ: whitelist ALLOWED_USERS (_is_allowed), fail-closed.

Фичи: мультичаты, персоны, авто-заголовки, follow-up кнопки-подсказки. Медиа: PDF/HTML-извлечение, Jina Reader, YouTube-транскрипты, голос (transcribe_audio через Gemini), генерация изображений.

Память: патч памяти применён/готовится; перед тем как считать фичу рабочей, нужно проверить команды /remember, /memory, /forget (или их фактические имена в коде), сохранение фактов в bot_state.json и подмешивание релевантных фактов в обычный ответ.

Медиа-конфиг: транскрипция — TRANSCRIBE_MODEL=gemini-3-flash-preview; картинки — IMAGE_MODEL=gpt-image-2 (сейчас 403 → форс Gemini gemini-3-pro-image-preview); PDF — обрезка до 20 000 символов; лимит Telegram TG_MAX=20 МБ.

🧠 /brain — оркестратор «Мегамозг»
Запускается только явно. Разбивает сложную задачу на подзадачи, считает смету, спрашивает подтверждение, выполняет линейный конвейер (v1, ≤5 подзадач) и собирает финальный ответ с кросс-проверкой.

Поток: /brain → _ask_input(action="brain") → планировщик (gpt_pro) возвращает JSON-план → гейт (≤1 шаг — отвечаем напрямую) → смета (_brain_estimate_cost) → кнопки ✅ Поехали / ✏️ Уточнить / ❌ Отмена → выполнение (_brain_execute) с прогресс-чеклистом → сборщик (gpt_pro) → критик _verify_answer.

Константы: BRAIN_ENABLED=1, BRAIN_BUDGET_USD=0.15, BRAIN_MAX_SUBTASKS=5, BRAIN_SUBTASK_TIMEOUT=180, планировщик BRAIN_PLANNER_MODEL=gpt-5.4, сборщик BRAIN_WRITER_MODEL=gpt-5.5. Исполнители (BRAIN_TYPE_MODEL): code → claude-opus-4-8, research/analysis → gpt-5.5, text → gemini-3.5-flash.

Особенности: таймаут подзадачи через ThreadPoolExecutor + future.result(timeout=…) (вотчдог), а не SIGALRM; v1 линейный, v2 (параллельный DAG) — план.

✅ Интеграция с Notion (задача: → Fable 5)
Обход «нельзя использовать ИИ-кредиты вне Notion»: бот ↔ агент через таблицу-почтовый-ящик.

Поток: задача: <текст> → notion_create_task создаёт строку в базе «Задачи от бота» (Задача title, Статус со значениями «Новая»/«В работе»/«Готово» — бот ставит только «Новая», ждёт «Готово»; «В работе» выставляет агент, Ответ, Приоритет) → кастомный агент Fable 5 триггерится на page.created, решает и пишет в Ответ, ставит Статус = Готово → бот опрашивает notion_get_answer (до ~10 мин) и шлёт ответ.

ENV: NOTION_TOKEN, NOTION_DB_ID. Нужно подключить интеграцию к базе (Connections).

🔀 Пул-ротатор воркспейсов
Надстройка над задача:: вместо одного воркспейса — пул, с ротацией ИИ-кредитов.

select_workspace (стратегия least_used по умолчанию / fill) выбирает воркспейс с остатком.
estimate_task_cost: light/medium/heavy = WS_COST_LIGHT/MEDIUM/HEAVY = 10/50/150; счётчики в ws_pool_state.json.
При исчерпании — ws_mark_exhausted и следующий. Лимит WS_CREDIT_LIMIT=300, WS_SOFT_FACTOR=0.95.
Секреты: NOTION_WS_JSON или индексные NOTION_WS_n_TOKEN/_DB/_NAME/_RESET_DAY.
У Notion нет API остатка кредитов → расход оценочный, калибровать WS_COST_*.

🔐 Безопасность, секреты и ENV
Перед масштабированием перевыпустить все засветившиеся секреты (ключ byesu, пароли прокси, Telegram-токен). Все ключи — только в HF Secrets, никогда в коде/репозитории.

SSRF-защита (_is_safe_public_url) в _fetch_url_text_impl (только публичные IP, проверка после редиректов, лимит 8 МБ).
Защита от prompt-injection из веба: WEB_GUIDANCE помечает веб-данные как недоверенные; цитаты добавляются кодом.
Whitelist ALLOWED_USERS (fail-closed) — единственный барьер доступа.
Полный список ENV (для переноса)
🔒 Реальные значения ключей и паролей намеренно не включены в этот файл. Заполняй их через Secrets окружения.

📦 Зависимости, тестирование и QA
requirements.txt (по реальным импортам)
Обязательные: pyTelegramBotAPI, openai, httpx, requests, Flask, huggingface_hub.
Каскад извлечения: trafilatura, readability-lxml, beautifulsoup4, lxml, pymupdf, pypdf.
Видео: youtube-transcript-api.
Тяжёлые опциональные (только при RERANK_ENABLED=1 / EMBED_ENABLED=1): sentence-transformers, numpy, torch; ONNX-путь — optimum[onnxruntime].
Ритуал QA перед деплоем
python -m py_compile app.py                       # компиляция без ошибок

# 1) Вычистить невидимые zero-width символы (U+200B и родня) — частая причина
#    SyntaxError: invalid non-printable character U+200B при вайбкоде/копипасте:
python -c "import io,re; p='app.py'; s=io.open(p,encoding='utf-8').read(); io.open(p,'w',encoding='utf-8').write(re.sub('[\u200b\u200c\u200d\u2060\ufeff]','',s))"

# 2) Проверка: ни битых (U+FFFD), ни невидимых символов не осталось:
python -c "import io; s=io.open('app.py',encoding='utf-8').read(); assert s.count('\ufffd')==0 and not any(c in s for c in '\u200b\u200c\u200d\u2060\ufeff'), 'есть невидимые/битые символы'"
Офлайн-тесты чистых функций (verify_search.py): canonicalize_url, _rrf_fuse, _simhash64/_dedup_simhash, _mmr_rank, парсинг _plan_subquestions, grounding-ветка _verify_answer.

Smoke в Telegram: «привет» (быстро при мёртвой Gemini), /research, /brain, задача:.

🎯 Видение и план развития
Бот остаётся личным; фокус смещается с экономии на качество.

Фича	Эффект	Сложность
Совет моделей (ensemble/best-of-N) + верификатор	Очень высокий, но дорого	Средняя; отложено
Режим наставника / ревью кода	Высокий (учёба)	Низкая
RAG по личному Notion	Высокий	Средняя–высокая
Долгая память + профиль	Средний–высокий	Средняя; патч применён — нужна проверка
Глубокий /research без жадности	Высокий	Низкая
Агенты по расписанию (дайджесты)	Средний	Средняя
Рекомендованный порядок: 1) проверить быстрый путь и память в живом коде; 2) закрыть оставшийся техдолг роутера; 3) RAG по Notion; 4) режим наставника / ревью кода; 5) /council — только потом, если реально упрёшься в качество ответов и будет не жалко бюджета.

Открытый техдолг (приоритетно)
🟡 Быстрый путь против «привет → дорогая модель»: патч, вероятно, применён; нужно верифицировать в коде и smoke-тестом. Цель: для тривиальных запросов — шаблон без LLM либо первая gpt-5.4-mini/haiku с дедлайном 5–8с, без дорогих «якорей».
🔴 Дешёвым/бесплатным каналам — короткий дедлайн (5–8с) и circuit на 10–15 мин после первого таймаута.
🟠 Запретить эскалацию дешёвая → дорогая для лёгких классов.
🟠 Гонки STATE/истории — immutable-snapshot истории на время генерации.
🟠 TTL-протечки: PENDING_ROUTE, CANCELS, MEDIA_GROUPS.
🟡 is_retriable слишком широкий — сузить до 429/5xx/таймаутов.
🟡 Persist circuit breaker (ModelHealth теряется при ребуте).
🚚 Чеклист переноса / деплоя
 Создать/дублировать HF Space; проверить порт 7860 / Flask keep-alive.
 Перенести все ENV/Secrets — с перевыпуском скомпрометированных ключей.
 Поднять Cloudflare Worker (TG_API_WORKER); убедиться, что apihelper.API_URL/FILE_URL переопределены.
 Настроить HF Dataset-бэкап состояния; при пуле — добавить ws_pool_state.json.
 Залить актуальную версию (app_brain_final.py) → py_compile + проверка U+FFFD → Factory reboot.
 Подключить интеграцию к базе «Задачи от бота» (Connections) во всех воркспейсах пула.
 Smoke-тесты: «привет», /research, /brain, задача:.
 Сверить расход по usage.csv и дашборду Notion credits.
📖 Глоссарий
byesu — платный OpenAI-совместимый шлюз (api.byesu.com) ко всем моделям; единственный платёжный канал.
Канал — ключ byesu со своим множителем цены; единица стоимости и отказа роутера.
Якорь — захардкоженная топ-модель поверх DEEP_FALLBACK_ORDER; причина бага «привет → Opus».
-c-модели — бесплатные Gemini-движки (нестриминговые, $0).
Вайбкод — код пишет ИИ (Codex), человек вставляет патчи.
Fable 5 — кастомный Notion-агент «Решатель задач от бота».
задача: — триггер: сообщение → строка в Notion → Fable 5 → ответ.
/brain («Мегамозг») — оркестратор: разбивка на подзадачи, смета, сборка ответа.
/research — глубокий веб-ресёрч (Поиск 2.0).
RRF — Reciprocal Rank Fusion, слияние ранжировок провайдеров.
MMR — Maximal Marginal Relevance, диверсификация выдачи.
ModelHealth / circuit breaker — пометка «мёртвых» моделей с кулдауном.
Cloudflare Worker — обратный прокси к Telegram.
Factory reboot — переразвёртывание HF Space после заливки кода.
Документ — живая сводка. При изменениях кода/архитектуры обновляй соответствующий слой и реестр фиксов.
