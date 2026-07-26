"""
Cross-service contract tests: training and streaming must agree exactly.

The most dangerous failure mode in this pipeline is silent. streaming-job
loads a PipelineModel that training-job fit against a specific ordered list
of feature columns, derived by a specific set of aggregation expressions
over a specific grouping key. If any of those three drift on one side only:

  * a missing column raises loudly at runtime — the good case;
  * a REORDERED column list does not raise. Spark scores every session with
    permuted features and returns confident nonsense;
  * a changed grouping key or aggregation expression does not raise either.
    The model is simply applied to a feature distribution it was never fit
    on, and nothing anywhere reports a problem.

Only the first case is self-announcing, so the other two are tested here.
These tests parse both source files with ast rather than importing them, so
they run in a plain Python environment with no pyspark, JVM, HDFS or MySQL.

    python -m pytest tests/ -v
"""

import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
TRAIN_PY = REPO_ROOT / "training-job" / "train.py"
CONSUMER_PY = REPO_ROOT / "streaming-job" / "consumer.py"

TRAIN_FN = "build_session_features"
CONSUMER_FN = "build_session_aggregates"


def _parse(path):
    return ast.parse(path.read_text(encoding="utf-8"))


def extract_module_constant(path, name):
    """Returns a module-level literal assignment without importing the module."""
    for node in _parse(path).body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == name:
                    return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found at module level in {path.name}")


def _function_node(path, name):
    for node in ast.walk(_parse(path)):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"function {name}() not found in {path.name}")


def _find_call(fn_node, method_name):
    for node in ast.walk(fn_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == method_name
        ):
            return node
    raise AssertionError(f".{method_name}() not found in {fn_node.name}()")


def aggregation_expressions(path, fn_name):
    """
    Returns {alias: normalized source} for every expression in the .agg()
    call, so the two services' aggregations can be compared structurally
    rather than by eyeballing two files side by side.
    """
    agg_call = _find_call(_function_node(path, fn_name), "agg")
    expressions = {}
    for arg in agg_call.args:
        # each arg looks like  <expr>.alias("name")
        assert isinstance(arg, ast.Call) and arg.func.attr == "alias", (
            f"unaliased aggregation in {path.name}: {ast.unparse(arg)}"
        )
        alias = ast.literal_eval(arg.args[0])
        expressions[alias] = ast.unparse(arg.func.value)
    return expressions


def grouping_key(path, fn_name):
    group_call = _find_call(_function_node(path, fn_name), "groupBy")
    return [ast.unparse(arg) for arg in group_call.args]


# --------------------------------------------------------------------------


def test_feature_columns_are_identical_and_identically_ordered():
    train_cols = extract_module_constant(TRAIN_PY, "FEATURE_COLS")
    consumer_cols = extract_module_constant(CONSUMER_PY, "FEATURE_COLS")

    assert train_cols == consumer_cols, (
        "FEATURE_COLS drifted. VectorAssembler is order-sensitive, so even a "
        "pure reordering silently corrupts every live score.\n"
        f"  train.py:    {train_cols}\n  consumer.py: {consumer_cols}"
    )


def test_feature_columns_have_no_duplicates():
    train_cols = extract_module_constant(TRAIN_PY, "FEATURE_COLS")
    assert len(train_cols) == len(set(train_cols))


def test_grouping_keys_match():
    """
    user_session alone is NOT equivalent to user_session + session_window.
    A session id in this dataset can span idle gaps, so grouping by id alone
    in training would merge windows that the streaming job scores separately.
    """
    train_key = grouping_key(TRAIN_PY, TRAIN_FN)
    consumer_key = grouping_key(CONSUMER_PY, CONSUMER_FN)

    assert train_key == consumer_key, (
        "Session grouping drifted between training and streaming.\n"
        f"  train.py:    {train_key}\n  consumer.py: {consumer_key}"
    )


def test_shared_aggregation_expressions_are_identical():
    """Every feature must be derived by the same expression on both sides."""
    train_aggs = aggregation_expressions(TRAIN_PY, TRAIN_FN)
    consumer_aggs = aggregation_expressions(CONSUMER_PY, CONSUMER_FN)

    # 'label' is training-only by design — it is the thing being predicted.
    train_features = {k: v for k, v in train_aggs.items() if k != "label"}

    assert set(train_features) == set(consumer_aggs), (
        "Aggregated column sets diverged.\n"
        f"  only in train.py:    {sorted(set(train_features) - set(consumer_aggs))}\n"
        f"  only in consumer.py: {sorted(set(consumer_aggs) - set(train_features))}"
    )

    mismatched = {
        alias: (train_features[alias], consumer_aggs[alias])
        for alias in train_features
        if train_features[alias] != consumer_aggs[alias]
    }
    assert not mismatched, f"Aggregation expressions diverged: {mismatched}"


def test_every_feature_excludes_purchase_events():
    """
    Leakage control, enforced per column rather than by the presence of a
    filter somewhere in the function. Counts of view/cart/remove are already
    purchase-free by construction; everything else must be guarded by the
    non_purchase condition.
    """
    train_aggs = aggregation_expressions(TRAIN_PY, TRAIN_FN)
    inherently_safe = {"view_count", "cart_count", "remove_count"}

    for alias, expr in train_aggs.items():
        if alias == "label" or alias in inherently_safe:
            continue
        assert "non_purchase" in expr, (
            f"Feature '{alias}' aggregates purchase events: {expr}\n"
            "This leaks the target into the feature set."
        )


def test_label_is_derived_from_all_events():
    """Only the FEATURES exclude purchases; the label still needs them."""
    train_aggs = aggregation_expressions(TRAIN_PY, TRAIN_FN)

    assert "label" in train_aggs, "train.py no longer produces a label column"
    assert "non_purchase" not in train_aggs["label"], (
        "The label must be computed across all events, otherwise it is "
        "always zero and the model has nothing to learn."
    )
    # ast.unparse normalizes string quoting, so match on the bare token.
    assert "purchase" in train_aggs["label"]


def test_consumer_produces_no_label():
    """A label at scoring time would mean the answer is already known."""
    consumer_aggs = aggregation_expressions(CONSUMER_PY, CONSUMER_FN)
    assert "label" not in consumer_aggs
