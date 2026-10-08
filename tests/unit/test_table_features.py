from src.common.table_features import (
    FEATURE_PROPERTIES,
    build_set_tblproperties_sql,
    ensure_table_features,
    missing_feature_properties,
    row_tracking_enabled,
)


class _Row(dict):
    pass


class FakeSpark:
    """Records SQL; answers SHOW TBLPROPERTIES from a fixed property map."""

    def __init__(self, props):
        self.props = dict(props)
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)
        spark = self

        class _Result:
            def collect(self_inner):
                if statement.startswith("SHOW TBLPROPERTIES"):
                    return [_Row(key=k, value=v) for k, v in spark.props.items()]
                return []

        return _Result()


def test_feature_properties_are_row_tracking_and_cdf():
    assert FEATURE_PROPERTIES == {
        "delta.enableRowTracking": "true",
        "delta.enableChangeDataFeed": "true",
    }


def test_row_tracking_enabled_variants():
    assert row_tracking_enabled({"delta.enableRowTracking": "true"})
    assert row_tracking_enabled({"delta.enableRowTracking": "TRUE"})
    assert not row_tracking_enabled({"delta.enableRowTracking": "false"})
    assert not row_tracking_enabled({})


def test_missing_when_nothing_set():
    assert missing_feature_properties({}) == FEATURE_PROPERTIES


def test_nothing_missing_when_all_true_any_case():
    props = {"delta.enableRowTracking": "TRUE", "delta.enableChangeDataFeed": "true"}
    assert missing_feature_properties(props) == {}


def test_false_counts_as_missing():
    props = {"delta.enableRowTracking": "true", "delta.enableChangeDataFeed": "false"}
    assert missing_feature_properties(props) == {"delta.enableChangeDataFeed": "true"}


def test_set_tblproperties_sql():
    sql = build_set_tblproperties_sql("c.s.t", FEATURE_PROPERTIES)
    assert sql == (
        "ALTER TABLE c.s.t SET TBLPROPERTIES ("
        "'delta.enableChangeDataFeed' = 'true', 'delta.enableRowTracking' = 'true')"
    )


def test_ensure_enables_only_missing_properties():
    spark = FakeSpark({"delta.enableRowTracking": "true"})
    result = ensure_table_features(spark, "c.s.t")
    assert result == {"table": "c.s.t", "status": "enabled",
                      "set": {"delta.enableChangeDataFeed": "true"}}
    assert spark.statements[-1] == (
        "ALTER TABLE c.s.t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def test_ensure_is_a_noop_when_present():
    spark = FakeSpark(FEATURE_PROPERTIES)
    result = ensure_table_features(spark, "c.s.t")
    assert result == {"table": "c.s.t", "status": "present", "set": {}}
    assert not any(s.startswith("ALTER") for s in spark.statements)
