from calista.table import CalistaEngine
import calista.core.functions as F

conf = {
    "credentials": {
    "account": "eo53442.europe-west2.gcp",
    "user": "mtsenkung",
    "password": "Aubay2024@92"
    }
}

data = {
    "a": [0, 1, 3, 3],
    "b": [0, 0, 0, 1]
}
snow = CalistaEngine(engine="spark").load_from_dict(data)
filter_r = F.mean_gt_value(col_name="a", value=2) & F.is_iban("c")
grouped_snow = snow.where(filter_r)
print(grouped_snow)



