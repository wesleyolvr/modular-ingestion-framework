class PipelineError(Exception):
    """
    Exceção lançada quando ocorre um erro no pipeline.
    """
    pass

class ValidationError(PipelineError):
    """
    Exceção lançada quando ocorre um erro de validação.
    """
    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__(f"Validation failed: {errors}")