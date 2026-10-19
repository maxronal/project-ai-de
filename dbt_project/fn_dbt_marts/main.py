import os
import subprocess
import functions_framework

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", "project-ai-de-ind26")

@functions_framework.http
def run_dbt_marts(request):
    try:
        dbt_dir = os.path.join(os.path.dirname(__file__), "dbt_project")

        # 1. Ejecutar solo modelos de la capa Marts (dim_customer, dim_product, fct_sales, customer_360)
        print("Ejecutando dbt run --select marts...")
        run_cmd = f"dbt run --select marts --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        run_out = subprocess.run(run_cmd, shell=True, capture_output=True, text=True)

        if run_out.returncode != 0:
            return {"status": "error", "phase": "marts run", "output": run_out.stderr or run_out.stdout}, 500

        # 2. Ejecutar tests sobre los Data Marts (not_null, unique, relationships)
        print("Ejecutando dbt test --select marts...")
        test_cmd = f"dbt test --select marts --project-dir {dbt_dir} --profiles-dir {dbt_dir}"
        test_out = subprocess.run(test_cmd, shell=True, capture_output=True, text=True)

        if test_out.returncode != 0:
            return {"status": "warning", "message": "Marts construidos con fallos en tests de relaciones", "output": test_out.stdout}, 200

        return {
            "status": "success",
            "layer": "marts",
            "message": "Capa Marts y Customer 360 procesados exitosamente."
        }, 200

    except Exception as e:
        return {"status": "error", "message": str(e)}, 500