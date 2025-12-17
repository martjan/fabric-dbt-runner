from datetime import datetime
import subprocess, shlex

class DbtRunner:
    def __init__(self, lakehouse_log_path: str, flush_every: int = 25):
        self.lakehouse_log_path = lakehouse_log_path
        self.flush_every = flush_every
        self.buffer = []
        self.persisted_log = ""   # <-- NEW
        self.success = True
        self.failure_type = None
        self.start_time = None

    # ---------- logging ----------
    def log(self, message: str, level: str = "INFO", end: str = "\n"):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted = f"[{timestamp}] [{level}] {message}"

        print(formatted, end=end)
        self.buffer.append(f"{formatted}{end}")

        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self):
        if not self.buffer:
            return

        try:
            chunk = "".join(self.buffer)
            self.buffer.clear()

            self.persisted_log += chunk

            notebookutils.fs.put(
                self.lakehouse_log_path,
                self.persisted_log,
                overwrite=True
            )

        except Exception as e:
            print(f"[WARN] Failed to flush logs: {e}")

    # ---------- dbt failure classification ----------
    def classify_failure(self, line: str):
        if "Compilation Error" in line:
            return "COMPILATION"
        if "Database Error" in line:
            return "DATABASE"
        if "Runtime Error" in line:
            return "RUNTIME"
        if "FAIL" in line:
            return "TEST_FAILURE"
        return None

    # ---------- execution ----------
    def run(self, cmd: str):
        self.start_time = datetime.now()
        self.log("Starting dbt run")

        process = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        for line in process.stdout:
            self.log(line, end="")

            if not self.failure_type:
                failure = self.classify_failure(line)
                if failure:
                    self.failure_type = failure

        return_code = process.wait()
        if return_code != 0:
            self.success = False
            self.log(
                f"dbt exited with return code {return_code}",
                level="ERROR"
            )

        self.flush()
        self.finish()        

        if not self.success:
            raise RuntimeError("dbt run failed")

    # ---------- finish ----------
    def finish(self):
        duration = (datetime.now() - self.start_time).total_seconds()

        self.log("")
        self.log("=" * 40)
        self.log("DBT Run Finished")
        self.log(f"Success: {self.success}")
        self.log(f"Duration: {duration:.1f}s")

        if self.failure_type:
            self.log(f"Failure type: {self.failure_type}", level="ERROR")

        self.log("=" * 40)

        self.flush()
