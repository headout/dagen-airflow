class TemplateNotFoundError(FileNotFoundError):
    def __init__(self, template_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template_id = template_id
        self.detail = f'Template not found with Template ID - "{template_id}"'

    def __str__(self):
        return self.detail
