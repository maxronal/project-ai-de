import os
import subprocess
import functions_framework

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "project-ai-de-ind26")

@functions_framework.http
def run_dbt_staging(request):
    try:
        dbt_dir = os.path.join(os.path.dirname(__file__), "dbt_project")

        # 1. Ejecutar solo modelos de la capa Staging
        print("Ejecutando dbt run --select staging...")
        run_cmd = f"dbt run --select staging --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        run_out = subprocess.run(run_cmd, shell=True, capture_output=True, text=True)

        if run_out.returncode != 0:
            return {"status": "error", "phase": "staging run", "output": run_out.stderr or run_out.stdout}, 500

        # 2. Ejecutar tests sobre la capa Staging
        print("Ejecutando dbt test --select staging...")
        test_cmd = f"dbt test --select staging --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        test_out = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)

        if test_out.returncode != 0:
            return {"status": "warning", "message": "Staging construido con fallos en pruebas", "output": test_out.stdout}, 200

        return {
            "status": "success",
            "layer": "staging",
            "message": "Capa Staging ejecutada y probada exitosamente."
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500