import yaml
cfg_path = "config.yaml"
with open(cfg_path, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f) or {}
# заменяем yandex_campaigns на нужный список
cfg["yandex_campaigns"] = [
    {"id": 1, "yandex_name": "Pesto2flight_Inlab"},
    # Добавляйте здесь другие кампании по мере необходимости
]
with open(cfg_path, "w", encoding="utf-8") as f:
    yaml.safe_dump(cfg, f, allow_unicode=True, sort_keys=False)
print("config.yaml обновлён")
