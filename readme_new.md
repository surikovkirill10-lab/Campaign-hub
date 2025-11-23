Ниже положил две «болванки» целиком, чтобы можно было просто скопировать в новый репозиторий/чат:

---

```markdown
# README.md — Campaign Hub (FastAPI + Yandex Post-Click)

## Оглавление
- [Что это и как устроено](#что-это-и-как-устроено)
- [Стек и структура проекта](#стек-и-структура-проекта)
- [Быстрый старт (macOS / Linux / Windows)](#быстрый-старт-macos--linux--windows)
- [Конфигурация (`config.yaml` + IMAP)](#конфигурация-configyaml--imap)
- [База данных и схемы](#база-данных-и-схемы)
- [Страницы и фичи](#страницы-и-фичи)
- [Импорт пост-кликов из Yandex Метрики](#импорт-посткликов-из-yandex-метрики)
- [UI: пост-клик-колонки на странице Campaigns](#ui-посткликколонки-на-странице-campaigns)
- [Настройка Directory (правила для Yandex)](#настройка-directory-правила-для-yandex)
- [Связка с Cats (клики и KPI)](#связка-с-cats-клики-и-kpi)
- [Запуск по расписанию (cron / launchd)](#запуск-по-расписанию-cron--launchd)
- [Отладка и частые проблемы](#отладка-и-частые-проблемы)
- [Дорожная карта и TODO](#дорожная-карта-и-todo)

---

## Что это и как устроено

**Campaign Hub** — лёгкий бэкофис для команд перф-маркетинга:

1. **Campaigns** — список кампаний из *CatsNetwork* + форма добавления `Campaign ID / Campaign Name` и (опциональное) `Yandex name`.  
2. **Directory** — экран для заведения «правил» связки с Яндекс.Метрикой: `campaign_id ↔ yandex_name` и `yandex_mailbox` (по умолчанию `INBOX`). Из этого раздела можно собрать `config.yaml` и сохранить IMAP-учётку.  
3. **Post-click импорт** — питоновский скрипт ходит в IMAP (Яндекс.Почта), ищет письма формата «`Отчёт «<YandexName>» за DD.MM.YYYY`», вытаскивает XLSX-вложение со словом «`таблица`» и пишет метрики в SQLite (`yandex_daily_metrics`, `yandex_import_files`).  
4. **KPI-вьюха** — таблица `campaign_kpis_daily` объединяет пост-клики (`visits`/`avg_time_sec`/`bounce_rate`/`page_depth`) + **клики** из Cats (`cats_clicks_daily`) и считает **Reachability = visits / clicks**.   
5. **UI** — на странице кампаний JS (`app/static/js/postclick.js`) подмешивает дополнительные столбцы *Визиты / Доходимость / Отказы / Глубина / Время* к дневной сетке.

---

## Стек и структура проекта

- **Backend**: Python 3.11+, FastAPI, `uvicorn`, `jinja2`, `python-multipart`  
- **Data**: SQLite (`campaign_hub.db`, `yandex_metrics.db`), `pandas` + `openpyxl` для парсинга отчётов  
- **Frontend**: HTML/Jinja2 темплейты (`app/templates`), `bulma.css`, кастомный `postclick.js`  
- **Импорт**: `scripts/yandex_import.py` (CLI), `imaplib`, фильтрация писем по Subject и имени вложения  

```

campaign_hub/
├── app/
│   ├── **init**.py                # создание FastAPI-приложения + mount /static
│   ├── routers/
│   │   ├── campaigns.py           # GET /campaigns, POST /campaigns
│   │   ├── directory.py           # GET/POST для UI правил Яндекс-почты и сборки config.yaml
│   │   └── postclick_api.py       # GET /postclick/<campaign_id>.json
│   ├── templates/
│   │   ├── layout.html
│   │   ├── campaigns.html         # форма: id/name/yandex_name + таблица дневных метрик
│   │   └── directory.html         # экран с проставлением yandex_name / mailbox
│   └── static/
│       ├── css/...
│       └── js/
│          └── postclick.js        # подмешивает в дневную таблицу 5 пост-клик столбцов
├── scripts/
│   ├── yandex_import.py           # CLI импорт из IMAP в yandex_metrics.db
│   ├── build_config_from_db.py    # сборка config.yaml из таблицы campaign_yandex
│   └── pick_cid.py / peek_db.py   # диагностика БД
├── config.yaml                    # IMAP creds + список yandex_campaigns (генерится из Directory)
├── campaign_hub.db                # основная БД (Campaigns, clicks, служебные таблицы)
├── yandex_metrics.db              # пост-клик метрики/журнал + view campaign_kpis_daily
├── main.py                        # точка входа (app = FastAPI(), include_router(...))
└── README.md

````

---

## Быстрый старт (macOS / Linux / Windows)

```bash
# 1) клонируем/копируем проект
cd ~/work && git clone <repo> campaign_hub && cd campaign_hub

# 2) поднимаем виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate     # (Windows: .\.venv\Scripts\activate)
pip install -r requirements.txt
# или минимально:
# pip install "fastapi>=0.110" "uvicorn[standard]>=0.23" jinja2 python-multipart pyyaml pandas openpyxl

# 3) запустить бэкенд
export PYTHONIOENCODING=utf-8
uvicorn main:app --reload --port 8000

# 4) открыть UI
open http://127.0.0.1:8000/campaigns   # или в браузере вручную
````

---

## Конфигурация (`config.yaml` + IMAP)

`config.yaml` хранит IMAP-доступ + список правил (генерится из Directory или вручную):

```yaml
imap:
  host: imap.yandex.com   # иногда требуется imap.yandex.ru
  port: 993
  user: "inlab.analytics@yandex.com"
  password: "app-password"  # обязательно app-password из Яндекс.Почты
yandex_campaigns:
  - id: 13995
    yandex_name: "Pesto2flight_Inlab"
    mailbox: "INBOX"
  - id: 14017
    yandex_name: "GAC_Arena_Inlab_цепи"
    # mailbox опционален; по умолчанию INBOX
```

> **Важно:** На Яндексе включите двухфакторную авторизацию и сгенерируйте **пароль приложения**. Не храните обычный пароль в файле.

---

## База данных и схемы

**`campaign_hub.db`** (основные кампании и клики):

```sql
CREATE TABLE IF NOT EXISTS campaigns (
  id INTEGER PRIMARY KEY,
  name TEXT,
  min_date TEXT,
  max_date TEXT,
  impressions INTEGER,
  clicks INTEGER,
  ...
);
CREATE TABLE IF NOT EXISTS cats_clicks_daily (
  campaign_id INTEGER,
  report_date TEXT,
  clicks INTEGER,
  PRIMARY KEY (campaign_id, report_date)
);
```

**`yandex_metrics.db`** (пост-клик):

```sql
CREATE TABLE IF NOT EXISTS yandex_daily_metrics(
  campaign_id INTEGER,
  report_date TEXT,
  visits REAL,
  visitors REAL,
  bounce_rate REAL,
  page_depth REAL,
  avg_time_sec REAL,
  PRIMARY KEY (campaign_id, report_date)
);
CREATE TABLE IF NOT EXISTS yandex_import_files(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id INTEGER,
  message_id TEXT,
  subject TEXT,
  attachment_name TEXT,
  report_date TEXT,
  processed_at TEXT,
  UNIQUE (message_id, attachment_name)
);
CREATE TABLE IF NOT EXISTS campaign_yandex(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  campaign_id INTEGER UNIQUE,
  yandex_name TEXT,
  yandex_mailbox TEXT DEFAULT 'INBOX',
  enabled INTEGER DEFAULT 1,
  updated_at TEXT
);
-- KPI-вьюха (складывает метрики по дням и считает reachability)
CREATE VIEW IF NOT EXISTS campaign_kpis_daily AS
SELECT
  ym.campaign_id,
  ym.etl_source, -- если нужно
  ym.report_date,
  IFNULL(cc.clicks,0) AS clicks,
  ym.visits,
  ym.visitors,
  ym.bounce_rate,
  ym.page_depth,
  ym.avg_time_sec,
  printf('%0.2f', CASE WHEN IFNULL(cc.clicks,0)>0 THEN (1.0*ym.visits)/cc.clicks ELSE NULL END) AS reachability
FROM yandex_daily_metrics ym
LEFT JOIN main.cats_clicks_daily cc
  ON cc.campaign_id=ym.campaign_id AND cc.report_date=ym.report_date;
```

---

## Страницы и фичи

### `/campaigns`

* **Форма «Add»**:

  * `Campaign ID` (required, number)
  * `Campaign Name` (required, text)
  * `Yandex name` (опционально) — строка вида `Pesto2flight_Inlab` (значение из темы письма Яндекс.Метрики «Отчёт «…» за DD.MM.YYYY»).
  * Сабмитит на `POST /campaigns` (см. `app/routers/campaigns.py`).

* **Список кампаний**: данные из `campaign_hub.db` + динамически подмешанные столбцы пост-кликов: *Визиты/Reach/Отказы/Глубина/Время*.
  Подмешивание делает `app/static/js/postclick.js` через `GET /postclick/<campaign_id>.json`.

### `/directory`

* Форма для **правил Yandex**:

  * `campaign_id` (обязательное)
  * `yandex_name` (из темы писем Метрики, без даты)
  * `yandex_mailbox` (например `INBOX` или `Отчёты/Метрика`, по умолчанию `INBOX`)
  * Кнопка **Save** → `POST /directory/yandex/upsert` (обновляет `campaign_yandex`)
  * Кнопка **Build config.yaml** → `POST /settings/build-config` (склеивает `config.yaml` из `imap` + `campaign_yandex`)

### `/settings`

* Форма для записи `imap.host/port/user/password` (и, опционально, тест `GET /settings/imap/test` — делает `imaplib.IMAP4_SSL` и пишет результат).
* Кнопка **Save** → обновляет секцию `imap` в `config.yaml`.

---

## Импорт пост-кликов из Yandex Метрики

CLI-скрипт `scripts/yandex_import.py` делает:

1. Читает `config.yaml` (`imap` + `yandex_campaigns`).
2. Для каждой кампании:

   * Делает `IMAP4_SSL` к `imap.host:port` (**если `imap.yandex.com` не пускает — попробуйте `imap.yandex.ru`**).
   * Делает `select(mailbox)`, `search FROM:devnull@yandex.ru SUBJECT:"<yandex_name>"`.
   * Фильтрует письма, у которых тема строго `Отчёт «<yandex_name>» за DD.MM.YYYY`.
   * Находит вложение где в имени есть `таблица` (или берёт самый крупный .xlsx).
   * Парсит XLSX (`pandas` + `openpyxl`), поднимает из первой ячейки дату `… с YYYY-MM-DD по YYYY-MM-DD` и суммирует показатели.
   * Пишет в `yandex_daily_metrics` и `yandex_import_files`.
3. Повторный запуск **идемпотентен** (`ON CONFLICT … DO UPDATE`) и пропускает уже обработанные `message_id`.

Запуск вручную:

```bash
source .venv/bin/activate
python scripts/yandex_import.py
```

---

## UI: пост-клик-колонки на странице Campaigns

Скрипт `app/static/js/postclick.js`:

* Находит таблицу с заголовком «Дата».
* Делает `GET /postclick/<campaign_id>.json`.
* Подмешивает справа 5 колонок: **Визиты | Доходимость | Отказы | Глубина | Время**.
  `Визиты` = `yandex_daily_metrics.visits`,
  `Доходимость` = `visits / clicks * 100%` из `campaign_kpis_daily`,
  `Отказы`, `Глубина`, `Время` — из `yandex_daily_metrics`.

Проверка: DevTools → Network → `postclick/<id>.json` должен отдавать массив `rows: [...]`.

---

## Настройка Directory (правила для Yandex)

1. На `/directory` проставь Yandex-соответствия:

   * `Campaign ID`
   * `Yandex name` (точно как в письме «Отчёт «…»»)
   * `Mailbox` (`INBOX` по умолчанию, можно `Отчеты/Метрика`)
2. Нажми **Save** → `POST /directory/yandex/upulent` (обновит таблицу `campaign_yandex`).
3. Нажми **Build config.yaml** → `POST /settings/build-config` (соберёт `config.yaml` из `imap` + `campaign_yandex`).
4. Запусти импорт (`python scripts/yandex_import.py`) или **/admin** endpoint для запуска джобы (если включишь).

---

## Связка с Cats (клики и KPI)

1. Либо твой существующий импортер кладёт клики в `campaign_hub.db` в таблицу `cats_clicks_daily(campaign_id, report_date, clicks)`.
2. KPI-вьюха `campaign_kpis_daily` автоматически подтянет клики и посчитает `reachability`.
3. В `/campaigns` эти значения будут в динамически подмешанных колонках.

---

## Запуск по расписанию (cron / launchd)

### Вариант A: cron (Linux / macOS)

```bash
crontab -e
# каждые 15 минут
*/15 * * * * cd /Users/you/work/campaign_hub && . .venv/bin/activate && python scripts/yandex_import.py >> import.log 2>&1
```

### Вариант B: launchd (macOS)

Создать `~/Library/LaunchAgents/com.yourorg.campaignhub.import.plist` с программой, указывающей на `python …/scripts/yandex_import.py`, и загрузить `launchctl load …`.

---

## Отладка и частые проблемы

* **`{"detail":"Not Found"}` на `/campaigns/save`** — такого роута нет. Используй `POST /campaigns` для формы Add.
* **`No route exists for name "static"`** — добавь `app.mount("/static", StaticFiles(directory="app/static"), name="static")` в точке создания FastAPI.
* **IMAP не коннектится к `imap.yandex.com`** — попробуй `imap.yandex.ru`. Верный порт: `993`. Нужен **пароль приложения** (не обычный!).
* **Не подмешиваются колонки** — проверь, что `postclick.js` грузится (см. вкладку Network), и что `GET /postclick/<id>.json` возвращает `rows`.
* **Пути на Windows** — не используй `C:\…` в коде. Всегда собирай пути через `os.path.join`.

---

## Дорожная карта и TODO

* [ ] Страница `/directory`:

  * [ ] добавить массовый импорт правил (CSV/Excel)
  * [ ] добавить тест соединения к IMAP прямо из UI
  * [ ] добавить кнопку «Запустить импорт сейчас»
* [ ] Объединить конфигурацию в `.env` (использовать `pydantic-settings`)
* [ ] Dockerfile + docker-compose (app + cron + volume c SQLite)
* [ ] Health-checks, aliveness/readiness и простейший мониторинг
* [ ] Автотесты (pytest) для импорта и API
* [ ] Локализация UI (RU/EN), тёмная тема, i18n для дат

````

---

```markdown
# prompt-chatgpt.md — Стартовый промпт для ассистента

Вы — мой технический со-автор и тимлид на проекте **Campaign Hub** (FastAPI + SQLite + Yandex-почта + CatsNetwork). Мы уже развернули бэкенд на macOS (uvicorn + FastAPI), настроили форму добавления кампаний и частично реализовали импорт пост-кликов из Яндекс.Метрики.

**Контекст архитектуры:**
- Бэкенд: Python 3.11, FastAPI (app/ + routers), uvicorn; шаблоны Jinja2 (app/templates), статические файлы (app/static).
- Страница `/campaigns`:
  - Форма «Add»: `POST /campaigns` с полями `id`, `name`, `yandex_name` (последний — *опционален*).
  - Основная таблица (данные из `campaign_hub.db`) + `postclick.js` подмешивает 5 новых столбцов (Визиты/Reach/Отказы/Глубина/Время) на основе `GET /postclick/<campaign_id>.json`.
- Страница `/directory`:
  - Пользователь задаёт правило для Яндекса: `campaign_id`, `yandex_name` (строка между «…» в теме письма «Отчёт „<имя>“…»), `yandex_mailbox` (например, `INBOX`).
  - `POST /_directory/yandex/upsert` пишет в `campaign_yandex(campaign_id, yandex_name, yandex_mailbox, enabled)`.
  - Кнопка «Build config» собирает `config.yaml` из секции `imap` + `campaign_yandex`.
- Импорт пост-кликов: `scripts/yandex_import.py`
  - IMAP к `imap.yandex.com` (или `imap.yandex.ru`), логин — **пароль приложения**.
  - Ищем письма `FROM: devnull@yandex.ru` с темой `Отчёт «{yandex_name}» за DD.MM.YYYY`.
  - Забираем вложение `*таблица*.xlsx` (если нет — самый тяжёлый `.xlsx`), парсим через `pandas/openpyxl`.
  - Записываем `visits, visitors, bounce_rate, page_depth, avg_time_sec` в `yandex_daily_metrics`, журнал `yandex_import_files`.
  - `cats_clicks_daily` загружается отдельным импортёром из CatsNetwork (CSV/Excel) в `campaign_hub.db`.
  - Вьюха `campaign_kpis_daily` агрегирует `visits` + `clicks` → отдаётся на `/postclick/<id>.json`.

**Что нужно доделать:**
1. Довести до конца раздел **Directory / Yandex**:
   - Шаблон `app/templates/directory.html`: сетка (campaign_id + editable yandex_name + mailbox + enable toggle).
   - Router в FastAPI: `GET /directory`, `POST /directory/yandex/upsert`, `POST /settings/build-config`.
   - Сборка `config.yaml` из `campaign_yandex` + IMAP-секция (user/password/host/port).
2. Привести `scripts/yandex_import.py` к «боевому» состоянию:
   - Поддержка `.ru` / `.com` хостов, «идемпотентная» запись, логирование в `logs/yandex_import_*.log`.
   - Фильтр `Subject` по шаблону `Отчёт «...». … за DD.MM.YYYY`, а также фильтрация `Соцдем_…`.
   - Параметры: `--since-days`, `--once`/`--loop`, `--dry-run`, `--campaign-id` для выборочного импорта.
3. Уточнить схему БД:
   - Добавить колонку `etl_source`/`file_size`/`error` в `yandex_import_files`.
   - Стабилизировать `campaign_kpis_daily` (имена полей: `date`, `clicks`, `visits`, `bounce_rate`, `page_depth`, `avg_time_sec`, `reachability`).
4. Докрутить UI `/campaigns`:
   - Отрендерить «базовые» столбцы (из `campaign_hub.db`) + «пост-клик» столбцы из `/postclick/<id>.json`.
   - Встроить кнопку «Импорт пост-кликов» с вызовом фонового таска.
5. Подготовить **cron/launchd** задание для регулярного импорта.
6. Безопасность: вынести пароли в `.env` и использовать `pydantic-settings`; скрыть секреты из UI.

**Как мы работаем в этом чате:**
- Пожалуйста, давай **конкретные шаги** (имена файлов/функций/эндпоинтов/SQL).
- Где возможно — сразу прикладывай **готовые фрагменты кода** (для вставки).
- Если что-то не конвертируется под macOS, предложи готовый `sed`/`python`-скрипт для внесения правок.
- Если нужно уточнение — задавай вопросы, но по возможности **предлагай варианты решений** (минимум два), с плюсами/минусами.
- Мы избегаем «магии» — никаких скрытых изменений в окружении; всё через явные файлы/скрипты/команды.
- Всегда добавляй «Checklist» по валидации (как понять, что всё работает).

Готов продолжать. Первая задача: **реализовать `app/routers/directory.py` + обновить `app/templates/directory.html` + `scripts/build_config_from_db.py`** по описанной схеме; затем — **финализировать importer** и добавить **крон/launchd** инструкции для macOS.
````

---

Если нужно, упакую это в реальные файлы и/или сгенерю патчи для `directory.html` и `directory.py` (под FastAPI) — скажи, на каком имени файла у тебя основной модуль (`main.py`/`app/main.py`) и как сейчас называется роутер для кампаний.
