from importlib import import_module


def import_engine(
    engine_name: str, base_class_name: str = "Engine"
):  # TODO trouver un meilleur nom
    import_base_path = f"calista.engines.{engine_name}"
    prefix_class_name = engine_name[0].upper() + engine_name[1:]
    return getattr(
        import_module(import_base_path), f"{prefix_class_name}{base_class_name}"
    )
