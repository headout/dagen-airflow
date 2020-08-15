__loader = None


def get_template_loader():
    from dagen.loader import TemplateLoader  # Avoid circular imports

    global __loader
    if __loader is None:
        __loader = TemplateLoader()
    return __loader


def get_managed_dags():
    loader = get_template_loader()
    if not loader.template_classes:
        loader.load_templates()
    return loader.get_managed_dags()


def refresh_dagen_templates():
    loader = get_template_loader()
    loader.load_templates(only_if_updated=False)
