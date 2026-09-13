#!/usr/bin/env python3
"""Validate published schemas, real generated payloads, and Java/API contract compatibility.

Install tests/contracts/requirements.txt, then run this script from any directory.
This is offline contract validation; HTTP behavior, clock skew, and persistence
semantics are exercised separately by the Java and pipeline integration suites.
"""

from copy import deepcopy
import json
from pathlib import Path
import re
import sys

try:
    import yaml
    from jsonschema import Draft202012Validator, FormatChecker
    from openapi_spec_validator import OpenAPIV31SpecValidator
except ImportError as error:
    raise SystemExit(
        "Missing contract dependency. Run: python -m pip install -r "
        "tests/contracts/requirements.txt"
    ) from error

from load_generator import generate_events


ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts"
JAVA = ROOT / "services/api/src/main/java/io/pulseguard/api"
HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}
FORMATS = FormatChecker()


class ContractFailure(ValueError):
    """A useful, named compatibility failure for CI output."""


def require(condition, message):
    if not condition:
        raise ContractFailure(message)


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f"Duplicate JSON key: {key}")
        result[key] = value
    return result


class UniqueYamlLoader(yaml.SafeLoader):
    """Do not let duplicate path/method keys silently hide a documented route."""


def unique_yaml_mapping(loader, node, deep=False):
    loader.flatten_mapping(node)
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        require(key not in result, f"Duplicate YAML key {key!r} at line {key_node.start_mark.line + 1}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


UniqueYamlLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_yaml_mapping)


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_json_object)


def local_reference(document, value):
    """Resolve component references used by compatibility checks, without network IO."""
    seen = set()
    while isinstance(value, dict) and "$ref" in value:
        reference = value["$ref"]
        require(reference.startswith("#/"), f"Expected a local component reference, got {reference}")
        require(reference not in seen, f"Cyclic component reference: {reference}")
        seen.add(reference)
        value = document
        for segment in reference[2:].split("/"):
            value = value[segment.replace("~1", "/").replace("~0", "~")]
    return value


def ensure_local_files(value, base_file):
    """Keep checked-in contract references self-contained and deterministic in CI."""
    if isinstance(value, dict):
        reference = value.get("$ref", "")
        if reference and not reference.startswith("#"):
            filename = reference.split("#", 1)[0]
            require(":" not in filename, f"External schema reference is not checked in: {reference}")
            target = (base_file.parent / filename).resolve()
            require(target.is_relative_to(CONTRACTS), f"Schema reference escapes contracts/: {reference}")
            require(target.is_file(), f"Missing referenced schema: {reference}")
        for child in value.values():
            ensure_local_files(child, base_file)
    elif isinstance(value, list):
        for child in value:
            ensure_local_files(child, base_file)


def accepted(validator, instance, label):
    errors = list(validator.iter_errors(instance))
    if errors:
        error = errors[0]
        location = ".".join(str(part) for part in error.path) or "<root>"
        raise ContractFailure(f"Valid {label} was rejected at {location}: {error.message}")


def rejected(validator, instance, label, keyword):
    errors = list(validator.iter_errors(instance))
    require(errors, f"Malformed {label} was unexpectedly accepted")
    require(any(error.validator == keyword for error in errors),
            f"Malformed {label} failed for the wrong reason; expected {keyword}, got {[error.validator for error in errors]}")


def read_record(source, name):
    """Read the repository's literal Java record declarations, failing on unsupported syntax.

    This is intentionally limited to the record/annotation style used here; it
    does not pretend to replace Java compilation or Spring's runtime mappings.
    """
    record = re.search(rf"\brecord\s+{re.escape(name)}\s*\((.*?)\)\s*\{{", source, re.DOTALL)
    require(record is not None, f"Cannot find Java record {name}")
    fields = {}
    field_pattern = r"((?:@\w+(?:\([^)]*\))?\s*)*)([\w.<>]+)\s+(\w+)\s*(?=,|$)"
    for match in re.finditer(field_pattern, record.group(1)):
        annotations, field_type, field_name = match.groups()
        fields[field_name] = {"type": field_type, "annotations": annotations}
    require(fields, f"Cannot read fields of Java record {name}")
    return fields


def enum_values(source, name):
    declaration = re.search(rf"\benum\s+{re.escape(name)}\s*\{{([^}}]+)\}}", source)
    require(declaration is not None, f"Cannot find Java enum {name}")
    return {value.strip() for value in declaration.group(1).split(",")}


def java_routes():
    routes = {}
    mapping = re.compile(r'@(Get|Post|Put|Patch|Delete)Mapping(?:\("([^"\n]*)"\))?(?=\s)')
    for filename in ("TransactionController.java", "InvestigationController.java"):
        source = (JAVA / "web" / filename).read_text(encoding="utf-8")
        base = re.search(r'@RequestMapping\("([^"\n]+)"\)', source)
        require(base is not None, f"Cannot read controller base path in {filename}")
        methods = list(mapping.finditer(source))
        require(len(methods) == len(re.findall(r"@(Get|Post|Put|Patch|Delete)Mapping\b", source)),
                f"Unsupported mapping annotation in {filename}; update the contract extractor")
        for index, method in enumerate(methods):
            route = (base.group(1) + (method.group(2) or ""), method.group(1).lower())
            require(route not in routes, f"Duplicate Java route: {route}")
            end = methods[index + 1].start() if index + 1 < len(methods) else len(source)
            routes[route] = source[method.end():end]
    return routes


def check_java_compatibility(spec, transaction_schema):
    declared = {(path, method) for path, entry in spec["paths"].items()
                for method in entry if method in HTTP_METHODS}
    implemented = java_routes()
    require(declared == set(implemented),
            f"OpenAPI/Java routes differ: undocumented={set(implemented) - declared}, unimplemented={declared - set(implemented)}")
    bounds = re.compile(r'@RequestParam\(defaultValue\s*=\s*"(\d+)"\)\s*@Min\((\d+)\)\s*@Max\((\d+)\)\s+int\s+(\w+)')
    for (path, method), signature in implemented.items():
        operation = spec["paths"][path][method]
        parameters = [local_reference(spec, item) for item in operation.get("parameters", [])]
        for default, minimum, maximum, name in bounds.findall(signature):
            parameter = next((item for item in parameters if item.get("name") == name and item.get("in") == "query"), None)
            require(parameter is not None, f"Missing documented query parameter {method.upper()} {path}?{name}")
            expected = {"default": int(default), "minimum": int(minimum), "maximum": int(maximum)}
            actual = {key: parameter["schema"].get(key) for key in expected}
            require(actual == expected, f"Query bounds differ for {method.upper()} {path}?{name}: Java={expected}, OpenAPI={actual}")
        require("503" in operation["responses"], f"Storage failure response missing for {method.upper()} {path}")

    payload_source = (JAVA / "transaction/TransactionPayload.java").read_text(encoding="utf-8")
    payload = read_record(payload_source, "TransactionPayload")
    require(set(transaction_schema["properties"]) == set(payload), "Transaction schema fields differ from TransactionPayload")
    required_payload = {name for name, field in payload.items() if re.search(r"@Not(?:Null|Blank)\b", field["annotations"])}
    require(set(transaction_schema["required"]) == required_payload, "Transaction required fields differ from Java validation")
    for field, enum in (("currency", "Currency"), ("channel", "Channel")):
        require(set(transaction_schema["properties"][field]["enum"]) == enum_values(payload_source, enum),
                f"Transaction {field} enum differs from Java")

    review_source = (JAVA / "web/InvestigationController.java").read_text(encoding="utf-8")
    review_fields = read_record(review_source, "ReviewRequest")
    review = spec["paths"]["/api/v1/alerts/{id}/review"]["patch"]["requestBody"]["content"]["application/json"]["schema"]
    require(set(review["properties"]) == set(review_fields), "Review request fields differ from Java")
    required_review = {name for name, field in review_fields.items() if re.search(r"@Not(?:Null|Blank)\b", field["annotations"])}
    require(set(review["required"]) == required_review, "Review required fields differ from Java validation")
    for name, field in review_fields.items():
        size = re.search(r"@Size\(min\s*=\s*(\d+),\s*max\s*=\s*(\d+)\)", field["annotations"])
        if size:
            require(review["properties"][name].get("minLength") == int(size.group(1))
                    and review["properties"][name].get("maxLength") == int(size.group(2)),
                    f"Review {name} length limits differ from Java")
    investigation_source = (JAVA / "investigation/InvestigationService.java").read_text(encoding="utf-8")
    require(set(review["properties"]["status"]["enum"]) == enum_values(investigation_source, "ReviewStatus"),
            "Review status enum differs from Java")

    view_source = (JAVA / "transaction/TransactionView.java").read_text(encoding="utf-8")
    require(set(spec["components"]["schemas"]["TransactionView"]["properties"]) == set(read_record(view_source, "TransactionView")),
            "Documented transaction response fields differ from TransactionView")
    require(spec["components"]["securitySchemes"]["ApiKey"] == {"type": "apiKey", "in": "header", "name": "X-API-Key"},
            "Write authentication must remain the X-API-Key header")
    for path, method in implemented:
        security = spec["paths"][path][method].get("security", spec.get("security", []))
        if method in {"post", "patch", "put", "delete"}:
            require({"ApiKey": []} in security, f"Write operation lacks API key security: {method.upper()} {path}")
        else:
            require(not security, f"Read operation unexpectedly requires authentication: {method.upper()} {path}")
    print(f"PASS Java/OpenAPI compatibility: {len(implemented)} routes, required DTO fields, enums, query bounds and write authentication")
    return review


def check_transaction_examples(schema):
    validator = Draft202012Validator(schema, format_checker=FORMATS)
    examples = []
    for document in [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]:
        for index, block in enumerate(re.findall(r"```json\s*\n(.*?)\n```", document.read_text(encoding="utf-8"), re.DOTALL), 1):
            instance = json.loads(block, object_pairs_hook=unique_json_object)
            if isinstance(instance, dict) and ("transactionId" in instance or "schemaVersion" in instance):
                accepted(validator, instance, f"documented example {document.relative_to(ROOT)} block {index}")
                examples.append(instance)
    require(examples, "No documented transaction JSON example was found")
    generated = []
    for scenario in ("normal", "mixed"):
        generated.extend(generate_events(120, 42, "contract-check", "2026-09-13T06:00:00.123456Z", scenario))
    for index, event in enumerate(generated):
        accepted(validator, event, f"load_generator event {index}")
    print(f"PASS transaction examples: {len(examples)} documented and {len(generated)} actual load-generator events")

    baseline = examples[0]
    for amount in (1, 100_000_000_000):
        accepted(validator, {**baseline, "amountMinor": amount, "transactionId": "x" * 64}, f"amount/identifier boundary {amount}")
    mutations = [
        ("negative amount", "amountMinor", -1, "minimum"),
        ("zero amount", "amountMinor", 0, "minimum"),
        ("excessive amount", "amountMinor", 100_000_000_001, "maximum"),
        ("fractional minor units", "amountMinor", 1.5, "type"),
        ("string amount", "amountMinor", "750000", "type"),
        ("boolean amount", "amountMinor", True, "type"),
        ("unsupported version", "schemaVersion", 2, "const"),
        ("unsupported currency", "currency", "JPY", "enum"),
        ("lowercase currency", "currency", "usd", "enum"),
        ("unsupported channel", "channel", "ATM", "enum"),
        ("lowercase country", "country", "us", "pattern"),
        ("oversized country", "country", "USA", "pattern"),
        ("invalid identifier", "transactionId", "transaction/123", "pattern"),
        ("oversized identifier", "accountId", "x" * 65, "pattern"),
        ("empty merchant", "merchantId", "", "pattern"),
        ("missing timezone", "eventTime", "2026-09-13T06:00:00", "format"),
        ("impossible date", "eventTime", "2026-02-30T06:00:00Z", "format"),
        ("numeric event time", "eventTime", 1789280100, "type"),
    ]
    for label, field, value, keyword in mutations:
        rejected(validator, {**baseline, field: value}, label, keyword)
    for field in schema["required"]:
        missing = deepcopy(baseline)
        missing.pop(field, None)
        rejected(validator, missing, f"missing {field}", "required")
    rejected(validator, {**baseline, "cardNumber": "not-a-contract-field"}, "unexpected field", "additionalProperties")
    print(f"PASS transaction rejection cases: {len(mutations) + len(schema['required']) + 1}; numeric boundaries and RFC3339 date-time checks enabled")


def check_review_and_acknowledgement(spec, review):
    validator = Draft202012Validator(review, format_checker=FORMATS)
    decision = {"status": "INVESTIGATING", "note": "Checked the original transaction evidence.", "analyst": "Risk operations"}
    for status in ("OPEN", "INVESTIGATING", "RESOLVED"):
        accepted(validator, {**decision, "status": status}, f"review status {status}")
    count = 0
    for field in ("status", "note", "analyst"):
        missing = deepcopy(decision)
        del missing[field]
        rejected(validator, missing, f"review missing {field}", "required")
        count += 1
    for field, value, keyword in (("status", "IGNORED", "enum"), ("note", "", "minLength"),
                                   ("note", "x" * 1001, "maxLength"), ("analyst", "x", "minLength"),
                                   ("analyst", "x" * 65, "maxLength"), ("authenticatedIdentity", True, "additionalProperties")):
        rejected(validator, {**decision, field: value}, f"invalid review {field}", keyword)
        count += 1
    acknowledgement = spec["paths"]["/api/v1/transactions"]["post"]["responses"]["202"]["content"]["application/json"]["schema"]
    ack_validator = Draft202012Validator(acknowledgement)
    for duplicate in (False, True):
        accepted(ack_validator, {"transactionId": "txn-contract", "status": "ACCEPTED", "duplicate": duplicate}, "ingestion acknowledgement")
    rejected(ack_validator, {"transactionId": "txn-contract", "status": "ACCEPTED"}, "acknowledgement without duplicate flag", "required")
    rejected(ack_validator, {"transactionId": "txn-contract", "status": "SENT", "duplicate": False}, "premature delivery acknowledgement", "const")
    print(f"PASS review and ingestion responses: 5 valid examples and {count + 2} rejected contract violations")


def main():
    openapi_path = CONTRACTS / "openapi.yaml"
    spec = yaml.load(openapi_path.read_text(encoding="utf-8"), Loader=UniqueYamlLoader)
    transaction_path = CONTRACTS / "transaction.v1.schema.json"
    transaction_schema = load_json(transaction_path)
    ensure_local_files(spec, openapi_path)
    ensure_local_files(transaction_schema, transaction_path)
    require(spec.get("openapi") == "3.1.0", "The published contract must use OpenAPI 3.1.0")
    OpenAPIV31SpecValidator(spec, base_uri=openapi_path.as_uri()).validate()
    Draft202012Validator.check_schema(transaction_schema)
    ingest_schema = spec["paths"]["/api/v1/transactions"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    require(ingest_schema.get("$ref") == "./transaction.v1.schema.json", "HTTP ingestion must reference the canonical Kafka transaction schema")
    print("PASS OpenAPI 3.1 specification, external local references and JSON Schema Draft 2020-12")
    review = check_java_compatibility(spec, transaction_schema)
    check_transaction_examples(transaction_schema)
    check_review_and_acknowledgement(spec, review)
    print("PASS all contract checks; runtime HTTP, clock-skew and persistence behavior are tested by Java/pipeline suites")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAIL contract validation: {type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from error
