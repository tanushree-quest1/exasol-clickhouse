import os

CH_UDF_XML_TEMPLATE = """<functions>
    <function>
        <type>executable</type>
        <name>score_fraud_py</name>
        <return_type>Float32</return_type>
        <argument>
            <type>Float32</type>
            <name>amount</name>
        </argument>
        <argument>
            <type>Float32</type>
            <name>mcc</name>
        </argument>
        <argument>
            <type>Float32</type>
            <name>hour</name>
        </argument>
        <argument>
            <type>Float32</type>
            <name>is_cnp</name>
        </argument>
        <argument>
            <type>Float32</type>
            <name>is_contactless</name>
        </argument>
        <format>TabSeparated</format>
        <command>ch_executable_score.py</command>
        <execute_direct>true</execute_direct>
    </function>
</functions>
"""

# ClickHouse native SQL vectorized formula (alternative that requires 0 external processes)
CH_SQL_UDF_DDL = """
CREATE OR REPLACE FUNCTION score_fraud_sql AS (amount, mcc, hour, is_cnp, is_contactless) ->
    1.0 / (1.0 + exp(-1.0 * (
        amount * 0.0085 +
        mcc * 0.0012 +
        hour * 0.045 +
        is_cnp * 1.85 +
        is_contactless * -0.65 - 2.85
    )));
"""


def register_clickhouse_udf(client=None):
    """Registers ClickHouse UDF via native vectorized SQL or executable config."""
    print("[*] Registering ClickHouse UDFs...")
    if client:
        try:
            client.command(CH_SQL_UDF_DDL)
            print("[+] Successfully registered ClickHouse SQL UDF: score_fraud_sql()")
        except Exception as e:
            print(f"[-] ClickHouse UDF registration error: {e}")
    else:
        print("[+] ClickHouse SQL UDF DDL ready (runs natively in ClickHouse SIMD engine).")


if __name__ == "__main__":
    register_clickhouse_udf()
