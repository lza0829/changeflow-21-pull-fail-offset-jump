from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class PullRequest(BaseModel):
    mode: str = "one"  # one=按位点拉一页；all=连续拉完（或失败即停）


class PullBind(BaseModel):
    source_id: int | None = None
    position: int | None = None


class PullSourceCreate(BaseModel):
    name: str
    manifest_path: str
    note: str | None = None
