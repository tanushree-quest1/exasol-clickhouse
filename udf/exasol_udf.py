EXASOL_PYTHON_UDF_DDL = """
OPEN SCHEMA PAYLENS;

--/
CREATE OR REPLACE PYTHON3 SCALAR SCRIPT score_fraud_py(
    amount DOUBLE,
    mcc DOUBLE,
    hour_val DOUBLE,
    is_cnp DOUBLE,
    is_contactless DOUBLE
) RETURNS DOUBLE AS
import math

WEIGHTS = [0.0085, 0.0012, 0.045, 1.85, -0.65]
BIAS = -2.85

def run(ctx):
    try:
        amt = ctx.amount if ctx.amount is not None else 0.0
        m = ctx.mcc if ctx.mcc is not None else 0.0
        h = ctx.hour_val if ctx.hour_val is not None else 0.0
        cnp = ctx.is_cnp if ctx.is_cnp is not None else 0.0
        cl = ctx.is_contactless if ctx.is_contactless is not None else 0.0
        
        z = (amt * WEIGHTS[0] +
             m * WEIGHTS[1] +
             h * WEIGHTS[2] +
             cnp * WEIGHTS[3] +
             cl * WEIGHTS[4] +
             BIAS)
        return 1.0 / (1.0 + math.exp(-max(min(z, 20.0), -20.0)))
    except Exception:
        return 0.0
/
"""


def register_exasol_udf(conn=None):
    """Registers Exasol native Python container UDF."""
    print("[*] Registering Exasol native distributed Python UDF...")
    if conn:
        try:
            conn.execute(EXASOL_PYTHON_UDF_DDL)
            print("[+] Successfully registered Exasol Python UDF: PAYLENS.score_fraud_py()")
        except Exception as e:
            print(f"[-] Exasol UDF registration error: {e}")
    else:
        print("[+] Exasol Python UDF DDL ready (runs natively in Exasol distributed containers).")


if __name__ == "__main__":
    register_exasol_udf()
