from datetime import datetime

from fastapi import APIRouter, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


PIPELINE_DATA = [
    PipelineRun(
        id="run-001",
        repository="bts-bdp-assignment",
        branch="main",
        status="success",
        triggered_by="push",
        started_at=datetime(2026, 3, 10, 10, 0, 0),
        finished_at=datetime(2026, 3, 10, 10, 5, 30),
        stages=["lint", "test", "build", "deploy"],
    ),
    PipelineRun(
        id="run-002",
        repository="bts-bdp-assignment",
        branch="feat/add-endpoint",
        status="failure",
        triggered_by="pull_request",
        started_at=datetime(2026, 3, 9, 9, 0, 0),
        finished_at=datetime(2026, 3, 9, 9, 2, 0),
        stages=["lint", "test"],
    ),
    PipelineRun(
        id="run-003",
        repository="bts-bdp-assignment",
        branch="main",
        status="success",
        triggered_by="push",
        started_at=datetime(2026, 3, 8, 8, 0, 0),
        finished_at=datetime(2026, 3, 8, 8, 4, 0),
        stages=["lint", "test", "build"],
    ),
    PipelineRun(
        id="run-004",
        repository="bts-bdp-assignment",
        branch="feat/redis-cache",
        status="success",
        triggered_by="pull_request",
        started_at=datetime(2026, 3, 7, 7, 0, 0),
        finished_at=datetime(2026, 3, 7, 7, 3, 0),
        stages=["lint", "test", "build"],
    ),
    PipelineRun(
        id="run-005",
        repository="bts-bdp-assignment",
        branch="main",
        status="running",
        triggered_by="schedule",
        started_at=datetime(2026, 3, 6, 6, 0, 0),
        finished_at=None,
        stages=["lint"],
    ),
]

STAGE_DATA = {
    "run-001": [
        PipelineStage(name="lint", status="success", started_at=datetime(2026, 3, 10, 10, 0, 0), finished_at=datetime(2026, 3, 10, 10, 1, 0), logs_url="/api/s9/pipelines/run-001/stages/lint/logs"),
        PipelineStage(name="test", status="success", started_at=datetime(2026, 3, 10, 10, 1, 0), finished_at=datetime(2026, 3, 10, 10, 3, 0), logs_url="/api/s9/pipelines/run-001/stages/test/logs"),
        PipelineStage(name="build", status="success", started_at=datetime(2026, 3, 10, 10, 3, 0), finished_at=datetime(2026, 3, 10, 10, 4, 30), logs_url="/api/s9/pipelines/run-001/stages/build/logs"),
        PipelineStage(name="deploy", status="success", started_at=datetime(2026, 3, 10, 10, 4, 30), finished_at=datetime(2026, 3, 10, 10, 5, 30), logs_url="/api/s9/pipelines/run-001/stages/deploy/logs"),
    ],
    "run-002": [
        PipelineStage(name="lint", status="success", started_at=datetime(2026, 3, 9, 9, 0, 0), finished_at=datetime(2026, 3, 9, 9, 1, 0), logs_url="/api/s9/pipelines/run-002/stages/lint/logs"),
        PipelineStage(name="test", status="failure", started_at=datetime(2026, 3, 9, 9, 1, 0), finished_at=datetime(2026, 3, 9, 9, 2, 0), logs_url="/api/s9/pipelines/run-002/stages/test/logs"),
    ],
    "run-003": [
        PipelineStage(name="lint", status="success", started_at=datetime(2026, 3, 8, 8, 0, 0), finished_at=datetime(2026, 3, 8, 8, 1, 0), logs_url="/api/s9/pipelines/run-003/stages/lint/logs"),
        PipelineStage(name="test", status="success", started_at=datetime(2026, 3, 8, 8, 1, 0), finished_at=datetime(2026, 3, 8, 8, 2, 0), logs_url="/api/s9/pipelines/run-003/stages/test/logs"),
        PipelineStage(name="build", status="success", started_at=datetime(2026, 3, 8, 8, 2, 0), finished_at=datetime(2026, 3, 8, 8, 4, 0), logs_url="/api/s9/pipelines/run-003/stages/build/logs"),
    ],
    "run-004": [
        PipelineStage(name="lint", status="success", started_at=datetime(2026, 3, 7, 7, 0, 0), finished_at=datetime(2026, 3, 7, 7, 1, 0), logs_url="/api/s9/pipelines/run-004/stages/lint/logs"),
        PipelineStage(name="test", status="success", started_at=datetime(2026, 3, 7, 7, 1, 0), finished_at=datetime(2026, 3, 7, 7, 2, 0), logs_url="/api/s9/pipelines/run-004/stages/test/logs"),
        PipelineStage(name="build", status="success", started_at=datetime(2026, 3, 7, 7, 2, 0), finished_at=datetime(2026, 3, 7, 7, 3, 0), logs_url="/api/s9/pipelines/run-004/stages/build/logs"),
    ],
    "run-005": [
        PipelineStage(name="lint", status="running", started_at=datetime(2026, 3, 6, 6, 0, 0), finished_at=None, logs_url="/api/s9/pipelines/run-005/stages/lint/logs"),
    ],
}


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    results = PIPELINE_DATA

    if repository:
        results = [p for p in results if p.repository == repository]
    if status_filter:
        results = [p for p in results if p.status == status_filter]

    results = sorted(results, key=lambda p: p.started_at, reverse=True)

    start = page * num_results
    return results[start: start + num_results]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    if pipeline_id not in STAGE_DATA:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Pipeline not found")
    return STAGE_DATA[pipeline_id]
