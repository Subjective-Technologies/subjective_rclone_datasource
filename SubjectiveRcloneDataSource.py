import json
import os
import sys
import platform
import shutil
import sqlite3
import subprocess
import time
from configparser import ConfigParser
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse
from urllib.request import url2pathname

from subjective_abstract_data_source_package import SubjectiveDataSource
from brainboost_data_source_logger_package.BBLogger import BBLogger


class SubjectiveRcloneDataSource(SubjectiveDataSource):
    """
    Index files from rclone remotes into a consolidated SQLite catalog while
    emitting progress updates through the standard datasource progress channel.
    """

    def __init__(self, name=None, session=None, dependency_data_sources=None, subscribers=None, params=None):
        super().__init__(
            name=name,
            session=session,
            dependency_data_sources=dependency_data_sources or [],
            subscribers=subscribers,
            params=params,
        )
        params = params or {}

        raw_config = params.get("rclone_config_path") or os.getenv("RCLONE_CONFIG")
        default_config = os.path.join(os.path.expanduser("~"), ".config", "rclone", "rclone.conf")
        self.rclone_config_path = self._normalize_path(os.path.expanduser(raw_config or default_config))

        self.sqlite_path = os.path.expanduser(
            params.get("sqlite_path") or "my_files.sqlite"
        )

        drives_param = params.get("drives") or []
        if isinstance(drives_param, str):
            drives_param = [d.strip() for d in drives_param.split(",") if d.strip()]
        self.drives: List[str] = drives_param

        dirs_param = params.get("dirs") or params.get("directories") or []
        if isinstance(dirs_param, str):
            dirs_param = [d.strip() for d in dirs_param.split(",") if d.strip()]
        self.directories: List[str] = dirs_param or [""]
        self.auto_install_rclone = bool(params.get("auto_install_rclone", False))
        self.rclone_binary = self._resolve_rclone_binary(params.get("rclone_binary_path"))

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def fetch(self):
        start_time = time.time()
        self._log(f"Starting rclone indexing using config: {self.rclone_config_path}")

        self._validate_config()
        remotes = self._load_remotes()

        targets = self._resolve_targets(remotes)
        if not targets:
            raise ValueError("No rclone remotes found to index.")

        os.makedirs(os.path.dirname(self.sqlite_path) or ".", exist_ok=True)
        conn = self._init_db()

        total_items = self._count_all_items(targets)
        self.set_total_items(total_items)
        self.set_processed_items(0)
        self._emit_progress_safe()
        processed = 0

        try:
            for drive_name, dir_path in targets:
                processed = self._index_target(conn, drive_name, dir_path, processed, start_time)
        finally:
            conn.close()

        self.set_fetch_completed(True)
        self._emit_progress_safe()
        self._log(f"Finished indexing. Total items stored: {processed}")

        # Return a small summary payload
        return {
            "db_path": os.path.abspath(self.sqlite_path),
            "total_indexed": processed,
            "remotes": list({drive for drive, _ in targets}),
        }

    def get_icon(self) -> str:
        icon_path = os.path.join(os.path.dirname(__file__), "icon.svg")
        try:
            with open(icon_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            self._log(f"Error reading icon file: {e}")
            return ""

    def get_connection_data(self) -> dict:
        """
        Describe the connection fields KnowledgeHooks should request.
        - rclone_config_path: absolute path to the rclone.conf containing all remotes.
        - sqlite_path: path to the SQLite catalog file (defaults to ./my_files.sqlite).
        - drives (optional): comma-separated list of remotes to index; if empty, all remotes are used.
        - dirs (optional): comma-separated list of directories to index per remote; empty means root of each remote.
        """
        return {
            "connection_type": "RCLONE",
            # Use explicit field specs so the UI renders file pickers for file paths.
            "fields": [
                {
                    "name": "rclone_config_path",
                    "label": "Rclone Config File",
                    "type": "text",
                    "placeholder": "C:/path/to/rclone.conf",
                    "required": True,
                },
                {
                    "name": "sqlite_path",
                    "label": "SQLite Catalog File",
                    "type": "text",
                    "placeholder": "C:/brainboost/Subjective/com_subjective_userdata/com_subjective_context/my_files.sqlite",
                    "default": "my_files.sqlite",
                },
                {
                    "name": "drives",
                    "label": "Drives (comma separated)",
                    "type": "text",
                    "placeholder": "remote1, remote2",
                },
                {
                    "name": "dirs",
                    "label": "Dirs (comma separated)",
                    "type": "text",
                    "placeholder": "/folderA, /folderB",
                },
                {
                    "name": "auto_install_rclone",
                    "label": "Auto-install rclone if missing",
                    "type": "checkbox",
                    "default": False,
                },
                {
                    "name": "rclone_binary_path",
                    "label": "Custom rclone binary path (optional)",
                    "type": "text",
                    "placeholder": "C:/path/to/rclone.exe",
                },
            ],
        }

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _validate_config(self) -> None:
        if not os.path.exists(self.rclone_config_path):
            raise FileNotFoundError(f"rclone config not found: {self.rclone_config_path}")
        if not self._rclone_installed():
            if self.auto_install_rclone:
                self._log("rclone not found; attempting automatic installation.")
                self._try_install_rclone()
            if not self._rclone_installed():
                raise EnvironmentError("rclone binary not found on PATH.")

    def _load_remotes(self) -> Dict[str, Dict[str, str]]:
        parser = ConfigParser()
        parser.read(self.rclone_config_path)
        remotes = {section: dict(parser.items(section)) for section in parser.sections()}
        self._log(f"Found {len(remotes)} configured remotes.")
        return remotes

    def _resolve_targets(self, remotes: Dict[str, Dict[str, str]]) -> List[tuple]:
        selected_drives = self.drives or list(remotes.keys())
        missing = [d for d in selected_drives if d not in remotes]
        if missing:
            raise ValueError(f"Requested remotes not in config: {', '.join(missing)}")
        return [(drive, directory) for drive in selected_drives for directory in self.directories]

    def _init_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path)
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_path TEXT NOT NULL,
                drive TEXT NOT NULL,
                size INTEGER NOT NULL,
                modified_date TEXT,
                file_type TEXT,
                UNIQUE(full_path)
            )
            """
        )
        conn.commit()
        return conn

    def _count_all_items(self, targets: Iterable[tuple]) -> int:
        total = 0
        for drive_name, dir_path in targets:
            try:
                total += self._count_remote_items(drive_name, dir_path)
            except Exception as exc:
                self._log(f"Unable to count items for {drive_name}:{dir_path or '/'} ({exc}); continuing.")
        return total

    def _count_remote_items(self, drive_name: str, dir_path: str) -> int:
        clean_dir = dir_path.strip("/") if dir_path else ""
        target = f"{drive_name}:{clean_dir}" if clean_dir else f"{drive_name}:"
        cmd = [
            self.rclone_binary,
            "size",
            target,
            "--json",
            "--config",
            self.rclone_config_path,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or "Unknown rclone size error")
        size_data = json.loads(result.stdout or "{}")
        return int(size_data.get("count", 0))

    def _index_target(
        self,
        conn: sqlite3.Connection,
        drive_name: str,
        dir_path: str,
        processed: int,
        start_time: float,
    ) -> int:
        clean_dir = dir_path.strip("/") if dir_path else ""
        target = f"{drive_name}:{clean_dir}" if clean_dir else f"{drive_name}:"
        self._log(f"Listing {target}")

        cmd = [
            self.rclone_binary,
            "lsf",
            target,
            "--recursive",
            "--format",
            "ptsm",
            "--separator",
            "\t",
            "--config",
            self.rclone_config_path,
        ]

        cursor = conn.cursor()
        batch: List[tuple] = []
        batch_size = 500

        with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
            for line in proc.stdout or []:
                parts = line.rstrip("\n").split("\t")
                if not parts or not parts[0]:
                    continue

                rel_path = parts[0]
                file_type = parts[1] if len(parts) > 1 else ""
                size = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
                modified = parts[3] if len(parts) > 3 else None

                prefix = f"{clean_dir.rstrip('/')}/" if clean_dir else ""
                full_path = f"{drive_name}:{prefix}{rel_path}"
                batch.append((full_path, drive_name, size, file_type, modified))

                if len(batch) >= batch_size:
                    self._flush_batch(cursor, conn, batch)
                    batch.clear()

                processed += 1
                self._update_progress(processed, start_time)

            if proc.wait() != 0:
                stderr = proc.stderr.read().strip() if proc.stderr else ""
                raise RuntimeError(f"rclone lsf failed for {target}: {stderr}")

        if batch:
            self._flush_batch(cursor, conn, batch)

        return processed

    def _flush_batch(self, cursor: sqlite3.Cursor, conn: sqlite3.Connection, batch: List[tuple]) -> None:
        cursor.executemany(
            """
            INSERT OR REPLACE INTO files (full_path, drive, size, file_type, modified_date)
            VALUES (?, ?, ?, ?, ?)
            """,
            batch,
        )
        conn.commit()

    def _update_progress(self, processed: int, start_time: float) -> None:
        self.set_processed_items(processed)
        self.set_total_processing_time(time.time() - start_time)
        self._emit_progress_safe()

    def _emit_progress_safe(self) -> None:
        try:
            if hasattr(self, "_emit_progress"):
                self._emit_progress()
        except Exception:
            # Progress reporting should never break the indexing loop
            pass

    def _normalize_path(self, path: str) -> str:
        """
        Accept file:// URLs or plain paths and convert to a local filesystem path.
        Handles Windows-style file URLs such as file:///C:/path/to/conf.
        """
        if not path:
            return path
        path = path.strip()
        if path.lower().startswith("file://"):
            parsed = urlparse(path)
            # Handle file://localhost/ and file://C:/ style URLs safely on Windows.
            if parsed.netloc and parsed.netloc not in ("", "localhost"):
                if parsed.netloc.endswith(":"):
                    local_path = f"/{parsed.netloc}{parsed.path}"
                else:
                    local_path = f"//{parsed.netloc}{parsed.path}"
            else:
                local_path = parsed.path
            return os.path.normpath(url2pathname(local_path))
        return os.path.normpath(path)

    def _rclone_installed(self) -> bool:
        return bool(self.rclone_binary and os.path.exists(self.rclone_binary))

    def _resolve_rclone_binary(self, override: Optional[str]) -> str:
        if override:
            override = self._normalize_path(override)
            if os.path.exists(override):
                return override
        # Check plugin-local deps/bin/{platform}/ directory
        plugin_dir = os.path.dirname(__file__)
        if sys.platform == "win32":
            local_dep = os.path.join(plugin_dir, "deps", "bin", "windows", "rclone.exe")
        elif sys.platform == "darwin":
            local_dep = os.path.join(plugin_dir, "deps", "bin", "mac", "rclone")
        else:
            local_dep = os.path.join(plugin_dir, "deps", "bin", "linux", "rclone")
        if os.path.exists(local_dep):
            return local_dep
        found = shutil.which("rclone")
        if found:
            return found
        # Legacy fallback: bin/ directory
        local_bin = os.path.join(plugin_dir, "bin", "rclone.exe" if os.name == "nt" else "rclone")
        if os.path.exists(local_bin):
            return local_bin
        return "rclone"

    def _try_install_rclone(self) -> None:
        system = platform.system().lower()
        installers: List[List[str]] = []

        if system == "windows":
            installers = [
                ["winget", "install", "-e", "--id", "Rclone.Rclone"],
                ["choco", "install", "rclone", "-y"],
                ["scoop", "install", "rclone"],
            ]
        elif system == "linux":
            if shutil.which("apt-get"):
                installers.append(["apt-get", "update"])
                installers.append(["apt-get", "install", "-y", "rclone"])
            if shutil.which("dnf"):
                installers.append(["dnf", "install", "-y", "rclone"])
            if shutil.which("yum"):
                installers.append(["yum", "install", "-y", "rclone"])
            if shutil.which("pacman"):
                installers.append(["pacman", "-Sy", "--noconfirm", "rclone"])
            if shutil.which("zypper"):
                installers.append(["zypper", "--non-interactive", "install", "rclone"])
            if shutil.which("apk"):
                installers.append(["apk", "add", "rclone"])
        else:
            self._log(f"Auto-install not supported on platform: {system}")
            return

        if not installers:
            self._log("No supported package manager found for auto-install.")
            return

        use_sudo = False
        if system == "linux":
            try:
                use_sudo = hasattr(os, "geteuid") and os.geteuid() != 0 and shutil.which("sudo") is not None
            except Exception:
                use_sudo = False

        for cmd in installers:
            if use_sudo and cmd[0] not in ("apt-get", "dnf", "yum", "pacman", "zypper", "apk"):
                pass
            full_cmd = cmd
            if use_sudo:
                full_cmd = ["sudo", "-n"] + cmd
            self._log(f"Attempting rclone install: {' '.join(full_cmd)}")
            try:
                result = subprocess.run(full_cmd, capture_output=True, text=True)
                if result.returncode != 0:
                    stderr = result.stderr.strip()
                    if stderr:
                        self._log(f"Install command failed: {stderr}")
                if self._rclone_installed():
                    self._log("rclone installed successfully.")
                    return
            except Exception as exc:
                self._log(f"Install command error: {exc}")

    def _log(self, message: str) -> None:
        try:
            BBLogger.log(message)
        except Exception:
            print(message, flush=True)
