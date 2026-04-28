"""
任务状态管理
用于跟踪长时间运行的任务（如图谱构建）
"""

import uuid
import threading
import os
import json
import sqlite3
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, field

from ..utils.locale import t
from ..config import Config


class TaskStatus(str, Enum):
    """任务状态枚举"""
    PENDING = "pending"          # 等待中
    PROCESSING = "processing"    # 处理中
    COMPLETED = "completed"      # 已完成
    FAILED = "failed"            # 失败


@dataclass
class Task:
    """任务数据类"""
    task_id: str
    task_type: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    progress: int = 0              # 总进度百分比 0-100
    message: str = ""              # 状态消息
    result: Optional[Dict] = None  # 任务结果
    error: Optional[str] = None    # 错误信息
    metadata: Dict = field(default_factory=dict)  # 额外元数据
    progress_detail: Dict = field(default_factory=dict)  # 详细进度信息
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "progress": self.progress,
            "message": self.message,
            "progress_detail": self.progress_detail,
            "result": self.result,
            "error": self.error,
            "metadata": self.metadata,
        }


class TaskManager:
    """
    任务管理器
    线程安全的任务状态管理
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._task_lock = threading.Lock()
                    cls._instance._db_path = os.path.join(Config.UPLOAD_FOLDER, "tasks.sqlite3")
                    cls._instance._init_db()
        return cls._instance

    def _init_db(self):
        """初始化任务数据库"""
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    progress INTEGER NOT NULL DEFAULT 0,
                    message TEXT NOT NULL DEFAULT '',
                    result_json TEXT,
                    error TEXT,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    progress_detail_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_tasks_type_created ON tasks(task_type, created_at DESC)"
            )
            conn.commit()

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            task_type=row["task_type"],
            status=TaskStatus(row["status"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            progress=row["progress"] or 0,
            message=row["message"] or "",
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            error=row["error"],
            metadata=json.loads(row["metadata_json"] or "{}"),
            progress_detail=json.loads(row["progress_detail_json"] or "{}"),
        )

    def _get_task_no_lock(self, task_id: str) -> Optional[Task]:
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if not row:
                return None
            return self._row_to_task(row)
    
    def create_task(self, task_type: str, metadata: Optional[Dict] = None) -> str:
        """
        创建新任务
        
        Args:
            task_type: 任务类型
            metadata: 额外元数据
            
        Returns:
            任务ID
        """
        task_id = str(uuid.uuid4())
        now = datetime.now()

        with self._task_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO tasks (
                        task_id, task_type, status, created_at, updated_at,
                        progress, message, result_json, error, metadata_json, progress_detail_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        task_type,
                        TaskStatus.PENDING.value,
                        now.isoformat(),
                        now.isoformat(),
                        0,
                        "",
                        None,
                        None,
                        json.dumps(metadata or {}, ensure_ascii=False),
                        json.dumps({}, ensure_ascii=False),
                    ),
                )
                conn.commit()
        
        return task_id
    
    def get_task(self, task_id: str) -> Optional[Task]:
        """获取任务"""
        with self._task_lock:
            return self._get_task_no_lock(task_id)
    
    def update_task(
        self,
        task_id: str,
        status: Optional[TaskStatus] = None,
        progress: Optional[int] = None,
        message: Optional[str] = None,
        result: Optional[Dict] = None,
        error: Optional[str] = None,
        progress_detail: Optional[Dict] = None
    ):
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            progress: 进度
            message: 消息
            result: 结果
            error: 错误信息
            progress_detail: 详细进度信息
        """
        with self._task_lock:
            task = self._get_task_no_lock(task_id)
            if not task:
                return

            task.updated_at = datetime.now()
            if status is not None:
                task.status = status
            if progress is not None:
                task.progress = progress
            if message is not None:
                task.message = message
            if result is not None:
                task.result = result
            if error is not None:
                task.error = error
            if progress_detail is not None:
                task.progress_detail = progress_detail

            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    UPDATE tasks
                    SET status = ?, updated_at = ?, progress = ?, message = ?,
                        result_json = ?, error = ?, metadata_json = ?, progress_detail_json = ?
                    WHERE task_id = ?
                    """,
                    (
                        task.status.value,
                        task.updated_at.isoformat(),
                        int(task.progress),
                        task.message or "",
                        json.dumps(task.result, ensure_ascii=False) if task.result is not None else None,
                        task.error,
                        json.dumps(task.metadata or {}, ensure_ascii=False),
                        json.dumps(task.progress_detail or {}, ensure_ascii=False),
                        task_id,
                    ),
                )
                conn.commit()
    
    def complete_task(self, task_id: str, result: Dict):
        """标记任务完成"""
        self.update_task(
            task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message=t('progress.taskComplete'),
            result=result
        )
    
    def fail_task(self, task_id: str, error: str):
        """标记任务失败"""
        self.update_task(
            task_id,
            status=TaskStatus.FAILED,
            message=t('progress.taskFailed'),
            error=error
        )
    
    def list_tasks(self, task_type: Optional[str] = None) -> list:
        """列出任务"""
        with self._task_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row
                if task_type:
                    rows = conn.execute(
                        "SELECT * FROM tasks WHERE task_type = ? ORDER BY created_at DESC",
                        (task_type,),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT * FROM tasks ORDER BY created_at DESC"
                    ).fetchall()
                return [self._row_to_task(row).to_dict() for row in rows]
    
    def cleanup_old_tasks(self, max_age_hours: int = 24):
        """清理旧任务"""
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        
        with self._task_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """
                    DELETE FROM tasks
                    WHERE created_at < ?
                      AND status IN (?, ?)
                    """,
                    (
                        cutoff.isoformat(),
                        TaskStatus.COMPLETED.value,
                        TaskStatus.FAILED.value,
                    ),
                )
                conn.commit()

