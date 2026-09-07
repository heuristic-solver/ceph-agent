"""
cli.py
CLI test harness for the Ceph Ingestion Workflow Classifier.
"""

import sys
import os
import argparse
from ceph_classifier.classifier import WorkflowClassifier

def print_result_banner(res):
    width = 68
    print("\n" + "=" * width)
    print("  CEPH INGESTION WORKFLOW CLASSIFIER (PERCEPTION LAYER)")
    print("=" * width)
    print(f"  Target Item     : {res.item_path}")
    print(f"  Item Category   : {res.item_type.upper()}")
    print(f"  DECISION        : >>> {res.target_workflow} <<<")
    print(f"  Confidence      : {res.confidence:.0%}")
    print(f"  Decision Tier   : {res.decision_tier}")
    print(f"  Destination     : {res.target_destination}")
    print("-" * width)
    print("  FEATURE EXTRACTION PROFILE:")
    if res.metadata:
        print(f"    * Is Directory : {res.metadata.is_directory} (Files: {res.metadata.file_count}, Depth: {res.metadata.max_depth})")
        print(f"    * Size (MB)    : {res.metadata.size_mb} MB")
        print(f"    * MIME Type    : {res.metadata.mime_type}")
        print(f"    * Magic Header : {res.metadata.magic_signature}")
        if res.metadata.has_project_markers:
            print(f"    * POSIX Markers: {', '.join(res.metadata.detected_markers)}")
    print("-" * width)
    print("  TECHNICAL RATIONALE:")
    print(f"    {res.rationale}")
    if res.tuning_parameters:
        print("-" * width)
        print("  OPTIMIZED CEPH TUNING PARAMETERS:")
        for k, v in res.tuning_parameters.items():
            print(f"    - {k}: {v}")
    print("=" * width + "\n")

def main():
    parser = argparse.ArgumentParser(description="Ceph Ingestion Workflow Classifier")
    parser.add_argument("path", help="Path to file, directory, disk image, or dataset")
    parser.add_argument("--intent", "-i", default=None, help="Optional user intent string (e.g. 'transactional database')")
    parser.add_argument("--no-llm", action="store_true", help="Disable Tier 2 LLM fallback")

    args = parser.parse_args()
    classifier = WorkflowClassifier(enable_llm_fallback=not args.no_llm)
    
    try:
        result = classifier.classify(args.path, user_intent=args.intent)
        print_result_banner(result)
    except Exception as e:
        print(f"[!] Error classifying {args.path}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
