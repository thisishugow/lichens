class ExceptionBase(Exception):
    def __init__(self, msg: str | None = None, *args: object) -> None:
        super().__init__(*args)
        self.msg = msg
    def __repr__(self) -> str:
        return super().__repr__()
    

class DatabaseConnectingFailed(ExceptionBase):
    def __repr__(self) -> str:
        return f'Failed to connect to Database. Detail: {self.msg}'