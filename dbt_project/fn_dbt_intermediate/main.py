import os
import subprocess
import functions_framework

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "project-ai-de-ind26")

@functions_framework.http
def run_dbt_intermediate(request):
    try:
        dbt_dir = os.path.join(os.path.dirname(__file__), "dbt_project")

        # 1. Ejecutar solo modelos de la capa Intermediate
        print("Ejecutando dbt run --select intermediate...")
        run_cmd = f"dbt run --select intermediate --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        run_out = subprocess.run(run_cmd, shell=True, capture_output=True, text=True)

        if run_out.returncode != 0:
            return {"status": "error", "phase": "intermediate run", "output": run_out.stderr or run_out.stdout}, 500

        # 2. Ejecutar tests sobre la capa Intermediate
        print("Ejecutando dbt test --select intermediate...")
        test_cmd = f"dbt test --select intermediate --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        test_out = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)

        return {
            "status": "success",
            "layer": "intermediate",
            "message": "Capa Intermediate ejecutada exitosamente."
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500