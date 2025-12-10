import os
import json
import sys
from typing import Any, Dict

import pandas as pd
from pandas import DataFrame

from evidently.core.report import Report
from evidently.presets import DataDriftPreset

from PredictiveAnalytics.exception import PredictiveAnalyticsException
from PredictiveAnalytics.logger import logging
from PredictiveAnalytics.utils.main_utils import read_yaml_file, write_yaml_file
from PredictiveAnalytics.entity.artifact_entity import (
    DataIngestionArtifact,
    DataValidationArtifact,
)
from PredictiveAnalytics.entity.config_entity import DataValidationConfig
from PredictiveAnalytics.constants import SCHEMA_FILE_PATH


class DataValidation:

    def __init__(
        self,
        data_ingestion_artifact: DataIngestionArtifact,
        data_validation_config: DataValidationConfig,
    ) -> None:
        """
        Initialize the DataValidation component.

        Args:
            data_ingestion_artifact: Artifact containing paths to train/test data.
            data_validation_config: Configuration for data validation.
        """
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config: Dict[str,
                                      Any] = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)

    # -------------------------------------------------------------------------
    # Schema Validation Methods
    # -------------------------------------------------------------------------

    def validate_number_of_columns(self, df: DataFrame) -> bool:
        """
        Validate number of columns matches schema.
        """
        try:
            required_cols = len(self._schema_config["columns"])
            actual_cols = len(df.columns)
            status = required_cols == actual_cols

            logging.info(
                f"Column count validation: required={required_cols}, actual={actual_cols}, status={status}"
            )
            return status

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)

    def is_column_exist(self, df: DataFrame) -> bool:
        """
        Validate required numerical and categorical columns exist.
        """
        try:
            df_columns = set(df.columns)

            missing_numerical = [
                col for col in self._schema_config["numerical_columns"]
                if col not in df_columns
            ]
            missing_categorical = [
                col for col in self._schema_config["categorical_columns"]
                if col not in df_columns
            ]

            if missing_numerical:
                logging.info(f"Missing numerical columns: {missing_numerical}")

            if missing_categorical:
                logging.info(
                    f"Missing categorical columns: {missing_categorical}")

            return not (missing_numerical or missing_categorical)

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)

    # -------------------------------------------------------------------------
    # Helper Method
    # -------------------------------------------------------------------------

    @staticmethod
    def read_data(file_path: str) -> DataFrame:
        """
        Load a CSV file safely.
        """
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)

    # -------------------------------------------------------------------------
    # Drift Detection (Updated for Evidently 0.7.17)
    # -------------------------------------------------------------------------

    # def detect_dataset_drift(self, reference_df: DataFrame, current_df: DataFrame) -> bool:
    #     """
    #     Detect dataset drift using Evidently 0.7.17 new Report API.
    #     """
    #     try:
    #         drift_report = Report(metrics=[DataDriftPreset()])
    #         drift_report.run(reference_data=reference_df, current_data=current_df)

    #         report_dict = drift_report.as_dict()

    #         # Save full drift report to YAML
    #         write_yaml_file(
    #             self.data_validation_config.drift_report_file_path, report_dict
    #         )

    #         # Extract drift metrics from new Evidently schema
    #         result = report_dict["metrics"][0]["result"]

    #         n_features = result.get("number_of_columns")
    #         n_drifted = result.get("number_of_drifted_columns")
    #         dataset_drift = result.get("dataset_drift")

    #         logging.info(
    #             f"Drift Summary: {n_drifted}/{n_features} features drifted. "
    #             f"Dataset Drift Detected: {dataset_drift}"
    #         )

    #         return bool(dataset_drift)

    #     except Exception as e:
    #         raise PredictiveAnalyticsException(e, sys)

    def parse_evidently_metrics_to_profile_like(
            self,
            report_dict: Dict[str, Any],
            reference_df=None) -> Dict[str, Any]:
        """
        Convert Evidently 0.7.x 'metrics' list into a small dict with:
        - n_features
        - n_drifted_features
        - dataset_drift (boolean)

        - report_dict: loaded JSON produced by report.save_json()
        - reference_df: optional pandas DataFrame; used as authoritative column count
        """
        metrics = report_dict.get("metrics", [])
        # Find the DriftedColumnsCount metric entry
        drifted_metric = None
        for m in metrics:
            cfg_type = m.get("config", {}).get("type", "")
            if cfg_type.endswith(
                    "DriftedColumnsCount") or "DriftedColumnsCount" in m.get(
                        "metric_name", ""):
                drifted_metric = m
                break

        if drifted_metric is None:
            # If not found, fallback: no drift info
            n_features = reference_df.shape[
                1] if reference_df is not None else None
            return {
                "n_features": n_features,
                "n_drifted_features": 0,
                "dataset_drift": False,
                "drift_share": 0.0,
                "drift_count": 0
            }

        value = drifted_metric.get("value", {})
        count = value.get("count")
        share = value.get("share")

        # fallback to reference_df column count if share is zero or missing
        if reference_df is not None:
            n_features = reference_df.shape[1]
        else:
            # attempt to infer from count/share if possible
            if share and share != 0:
                n_features = int(round(count / share))
            else:
                n_features = None

        drift_share_threshold = drifted_metric.get("config",
                                                   {}).get("drift_share")
        # If config threshold missing, default to 0.5 (common)
        if drift_share_threshold is None:
            drift_share_threshold = 0.5

        dataset_drift = False
        if share is not None:
            dataset_drift = float(share) >= float(drift_share_threshold)

        return {
            "n_features": int(n_features) if n_features is not None else None,
            "n_drifted_features": int(count) if count is not None else 0,
            "dataset_drift": bool(dataset_drift),
            "drift_share": float(share) if share is not None else 0.0,
            "drift_count": int(count) if count is not None else 0,
            "drift_share_threshold": float(drift_share_threshold)
        }

    # ************************************************************

    def detect_dataset_drift(self, reference_df: DataFrame,
                             current_df: DataFrame) -> bool:
        """
        Run Evidently DataDriftPreset, save JSON, parse metrics and return boolean drift flag.
        """
        try:
            # Build & run the report
            drift_report = Report(metrics=[DataDriftPreset()])
            rendered = drift_report.run(reference_data=reference_df,
                                        current_data=current_df)

            # Save the raw report JSON to the configured path (this matches what you used)
            json_path = self.data_validation_config.drift_report_file_path
            os.makedirs(os.path.dirname(json_path), exist_ok=True)
            rendered.save_json(json_path)

            # Load the JSON back into a dict
            with open(json_path, "r", encoding="utf-8") as f:
                report_dict = json.load(f)

            # Parse new metrics list into old-style numbers
            parsed = self.parse_evidently_metrics_to_profile_like(
                report_dict, reference_df=reference_df)

            # Build a small report dict in the old 'profile' shape if other code expects that nesting
            profile_like = {
                "data_drift": {
                    "data": {
                        "metrics": {
                            "n_features":
                            parsed["n_features"],
                            "n_drifted_features":
                            parsed["n_drifted_features"],
                            "dataset_drift":
                            parsed["dataset_drift"],
                            "drift_share":
                            parsed["drift_share"],
                            "drift_share_threshold":
                            parsed["drift_share_threshold"]
                        }
                    }
                },
                # also keep the raw metrics for completeness
                "raw_metrics": report_dict.get("metrics", []),
            }

            # Save YAML (your pipeline expects YAML); write_yaml_file should accept dicts
            # write_yaml_file(file_path=json_path.replace(".json", ".yaml"), content=profile_like)
            # Or if you MUST write to same drift_report_file_path, use it:
            # write_yaml_file(
            #     file_path=self.data_validation_config.drift_report_file_path,
            #     content=profile_like)

            # write_yaml_file(self.data_validation_config.drift_report_file_path, report_dict)

            yaml_path = json_path.replace(".json", ".yaml")
            os.makedirs(os.path.dirname(yaml_path), exist_ok=True)
            write_yaml_file(file_path=yaml_path, content=profile_like)

            # Log and return
            logging.info(
                f"{parsed['n_drifted_features']}/{parsed['n_features']} drifted "
                f"(share={parsed['drift_share']}, threshold={parsed['drift_share_threshold']}). "
                f"Dataset drift: {parsed['dataset_drift']}")

            return bool(parsed["dataset_drift"])

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys) from e

    # -------------------------------------------------------------------------
    # Pipeline Orchestration
    # -------------------------------------------------------------------------

    def initiate_data_validation(self) -> DataValidationArtifact:
        """
        Execute data validation: schema validation and drift detection.
        """
        try:
            logging.info("Starting data validation process...")

            # Load data
            train_df = self.read_data(
                self.data_ingestion_artifact.trained_file_path)
            test_df = self.read_data(
                self.data_ingestion_artifact.test_file_path)

            error_messages = []

            # Column count validation
            if not self.validate_number_of_columns(train_df):
                error_messages.append("Training data: Column count mismatch.")
            if not self.validate_number_of_columns(test_df):
                error_messages.append("Test data: Column count mismatch.")

            # Column existence validation
            if not self.is_column_exist(train_df):
                error_messages.append(
                    "Training data: Missing required columns.")
            if not self.is_column_exist(test_df):
                error_messages.append("Test data: Missing required columns.")

            validation_passed = len(error_messages) == 0

            drift_status = False
            if validation_passed:
                drift_status = self.detect_dataset_drift(train_df, test_df)

            message = ("Drift detected"
                       if drift_status else "Drift not detected"
                       if validation_passed else "; ".join(error_messages))

            artifact = DataValidationArtifact(
                validation_status=validation_passed,
                message=message,
                drift_report_file_path=self.data_validation_config.
                drift_report_file_path,
            )

            logging.info(f"Data validation completed. Artifact: {artifact}")
            return artifact

        except Exception as e:
            raise PredictiveAnalyticsException(e, sys)
