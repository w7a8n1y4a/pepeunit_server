import datetime
from math import ceil, floor
from numbers import Real
from typing import ClassVar

from pydantic import BaseModel, Field, ValidationError, model_validator

from app import runtime, settings
from app.configs.error_codes import ErrorCode
from app.configs.errors import DataPipeError
from app.dto.enum import (
    ActivePeriodType,
    AggregationFunctions,
    DataPipeStage,
    DataPipeTransformationField,
    FieldKey,
    FieldType,
    FilterTypeValueFiltering,
    FilterTypeValueThreshold,
    ProcessingPolicyType,
    TypeInputValue,
)
from app.schemas.pydantic.unit_io import (
    DataPipeConfigOptionsRead,
    DataPipeModeOptions,
    DataPipeNumericRange,
    DataPipeValidationErrorRead,
)
from app.schemas.pydantic.unit_manifest import FieldDeclaration
from app.utils.utils import snake_to_camel


class ActivePeriod(BaseModel):
    type: ActivePeriodType
    start: datetime.datetime | None = None
    end: datetime.datetime | None = None

    @model_validator(mode="after")
    def check_active_period(cls, self):
        if self.type == ActivePeriodType.FROM_DATE and not self.start:
            msg = "start must be provided for FROM_DATE"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_ACTIVE_FROM_DATE_START, msg
            )
        if self.type == ActivePeriodType.TO_DATE and not self.end:
            msg = "end must be provided for TO_DATE"
            raise ErrorCode.coded(ErrorCode.DATAPIPE_ACTIVE_TO_DATE_END, msg)
        if self.type == ActivePeriodType.DATE_RANGE:
            if not self.start or not self.end:
                msg = "start and end must be provided for DATE_RANGE"
                raise ErrorCode.coded(
                    ErrorCode.DATAPIPE_ACTIVE_RANGE_BOUNDS, msg
                )
            if self.start >= self.end:
                msg = "start must be before end for DATE_RANGE"
                raise ErrorCode.coded(
                    ErrorCode.DATAPIPE_ACTIVE_RANGE_ORDER, msg
                )
        return self


def validate_filtering_values(
    type_input_value: TypeInputValue,
    type_value_filtering: FilterTypeValueFiltering | None,
    filtering_values: list[str | int | float] | None,
):
    """Validate filtering values based on input type."""
    if not (type_value_filtering and filtering_values):
        return

    if type_input_value == TypeInputValue.NUMBER and not all(
        isinstance(x, Real) for x in filtering_values
    ):
        msg = "filtering_values must be numeric for NUMBER input"
        raise ErrorCode.coded(ErrorCode.DATAPIPE_FILTER_VALUES_NUMBER, msg)
    if type_input_value == TypeInputValue.TEXT and not all(
        isinstance(x, str) for x in filtering_values
    ):
        msg = "filtering_values must be strings for TEXT input"
        raise ErrorCode.coded(ErrorCode.DATAPIPE_FILTER_VALUES_TEXT, msg)


def validate_thresholds(
    type_input_value: TypeInputValue,
    type_value_threshold: FilterTypeValueThreshold | None,
    threshold_min: float | None,
    threshold_max: float | None,
):
    """Validate threshold configuration for numeric input."""
    if type_input_value != TypeInputValue.NUMBER:
        return

    if (
        type_value_threshold == FilterTypeValueThreshold.MIN
        and threshold_min is None
    ):
        msg = "threshold_min is required for MIN threshold"
        raise ErrorCode.coded(ErrorCode.DATAPIPE_THRESHOLD_MIN_REQUIRED, msg)

    if (
        type_value_threshold == FilterTypeValueThreshold.MAX
        and threshold_max is None
    ):
        msg = "threshold_max is required for MAX threshold"
        raise ErrorCode.coded(ErrorCode.DATAPIPE_THRESHOLD_MAX_REQUIRED, msg)

    if type_value_threshold == FilterTypeValueThreshold.RANGE:
        if threshold_min is None or threshold_max is None:
            msg = "Both threshold_min and threshold_max are required for RANGE threshold"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_THRESHOLD_RANGE_REQUIRED, msg
            )
        if threshold_min >= threshold_max:
            msg = "threshold_min must be less than threshold_max"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_THRESHOLD_RANGE_ORDER, msg
            )


class FiltersConfig(BaseModel):
    type_input_value: TypeInputValue

    type_value_filtering: FilterTypeValueFiltering | None = None
    filtering_values: list[str | int | float] | None = None

    type_value_threshold: FilterTypeValueThreshold | None = None
    threshold_min: int | None = None
    threshold_max: int | None = None

    max_rate: int = Field(ge=0, le=86400)
    last_unique_check: bool = False
    max_size: int = Field(ge=0)

    def _validate_max_size(self):
        """Validate max_size against MQTT payload limit."""
        max_allowed_size = runtime.get().pu_mqtt_max_payload_size * 1024
        if self.max_size > max_allowed_size:
            msg = f"max_size must be <= {max_allowed_size}"
            raise ErrorCode.coded(ErrorCode.DATAPIPE_MAX_SIZE, msg)

    @model_validator(mode="after")
    def validate_filters(self):
        validate_filtering_values(
            self.type_input_value,
            self.type_value_filtering,
            self.filtering_values,
        )
        validate_thresholds(
            self.type_input_value,
            self.type_value_threshold,
            self.threshold_min,
            self.threshold_max,
        )
        self._validate_max_size()
        return self

    def check_numeric_limits(
        self, numeric_range: DataPipeNumericRange
    ) -> list[DataPipeValidationErrorRead]:
        lower = numeric_range.min
        upper = numeric_range.max
        values = []
        if self.type_value_filtering and self.filtering_values:
            values.extend(
                (f"filtering_values[{index}]", value)
                for index, value in enumerate(self.filtering_values)
            )
        if self.type_value_threshold in (
            FilterTypeValueThreshold.MIN,
            FilterTypeValueThreshold.RANGE,
        ):
            values.append(("threshold_min", self.threshold_min))
        if self.type_value_threshold in (
            FilterTypeValueThreshold.MAX,
            FilterTypeValueThreshold.RANGE,
        ):
            values.append(("threshold_max", self.threshold_max))

        issues = []
        for name, value in values:
            if isinstance(value, Real) and not lower <= value <= upper:
                issues.append(
                    DataPipeValidationErrorRead(
                        stage=DataPipeStage.FILTERS,
                        message=f"{name} must be within the UnitIO range [{lower}, {upper}]",
                        error_code=ErrorCode.DATAPIPE_IO_RANGE_MISMATCH,
                    )
                )
        return issues


class TransformationConfig(BaseModel):
    NUMBER_FIELDS: ClassVar[tuple[DataPipeTransformationField, ...]] = (
        DataPipeTransformationField.MULTIPLICATION_RATIO,
        DataPipeTransformationField.ROUND_DECIMAL_POINT,
    )
    TEXT_FIELDS: ClassVar[tuple[DataPipeTransformationField, ...]] = (
        DataPipeTransformationField.SLICE_START,
        DataPipeTransformationField.SLICE_END,
    )

    multiplication_ratio: float | None = None
    round_decimal_point: int | None = Field(default=None, ge=0, le=7)
    slice_start: int | None = None
    slice_end: int | None = None


class ProcessingPolicyConfig(BaseModel):
    policy_type: ProcessingPolicyType
    n_records_count: int | None = None
    time_window_size: int | None = None
    aggregation_functions: AggregationFunctions | None = None

    @model_validator(mode="after")
    def validate_processing(self):
        if self.policy_type == ProcessingPolicyType.N_RECORDS:
            if self.n_records_count is None:
                msg = "n_records_count is required for N_RECORDS"
                raise ErrorCode.coded(
                    ErrorCode.DATAPIPE_N_RECORDS_REQUIRED, msg
                )
            if not (0 < self.n_records_count <= 1024):
                msg = "n_records_count must be between 1 and 1024"
                raise ErrorCode.coded(ErrorCode.DATAPIPE_N_RECORDS_RANGE, msg)

        if self.policy_type in [
            ProcessingPolicyType.TIME_WINDOW,
            ProcessingPolicyType.AGGREGATION,
        ]:
            if self.time_window_size is None:
                msg = "time_window_size is required"
                raise ErrorCode.coded(
                    ErrorCode.DATAPIPE_TIME_WINDOW_REQUIRED, msg
                )
            if self.time_window_size not in settings.pu_time_window_sizes:
                msg = f"Invalid time_window_size. Must be one of: {settings.pu_time_window_sizes}"
                raise ErrorCode.coded(
                    ErrorCode.DATAPIPE_TIME_WINDOW_INVALID, msg
                )

        if (
            self.policy_type == ProcessingPolicyType.AGGREGATION
            and self.aggregation_functions is None
        ):
            msg = "aggregation_functions is required for AGGREGATION"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_AGGREGATION_FUNCTIONS_REQUIRED, msg
            )

        return self


class AlertsConfig(BaseModel):
    """Alert rules use the same acceptance as filters: a value an equivalent
    filter would keep raises an alert. With neither rule set, every value
    alerts. Values are typed by filters.type_input_value, so the type
    dependent checks live in DataPipeConfig.
    """

    is_enabled: bool = True

    # WhiteList alerts on a value in the list, BlackList on a value outside it
    type_value_filtering: FilterTypeValueFiltering | None = None
    filtering_values: list[str | int | float] | None = None

    # Min alerts at or above threshold_min, Max at or below threshold_max,
    # Range inside both
    type_value_threshold: FilterTypeValueThreshold | None = None
    threshold_min: int | None = None
    threshold_max: int | None = None

    # Matches in a row required before the first alert
    consecutive_count: int = Field(default=1, ge=1, le=1024)

    # Minimum seconds between two alerts of the same node
    max_frequency: int = Field(default=10, ge=10, le=86400)

    @model_validator(mode="after")
    def validate_alerts(self):
        if self.type_value_filtering and not self.filtering_values:
            msg = f"filtering_values is required for {self.type_value_filtering.value} filtering"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_FILTERING_VALUES_REQUIRED, msg
            )
        return self


class DataPipeConfig(BaseModel):
    active_period: ActivePeriod
    filters: FiltersConfig
    transformations: TransformationConfig | None = None
    processing_policy: ProcessingPolicyConfig
    alerts: AlertsConfig | None = None

    @model_validator(mode="after")
    def validate_alerts_input_type(self):
        # Errors raised here have no field location, see format_validation_error_dict
        if self.alerts is None:
            return self

        type_input_value = self.filters.type_input_value
        if (
            type_input_value == TypeInputValue.TEXT
            and self.alerts.type_value_threshold is not None
        ):
            msg = "type_value_threshold is not applicable to TEXT input"
            raise ErrorCode.coded(
                ErrorCode.DATAPIPE_THRESHOLD_NOT_FOR_TEXT, msg
            )
        validate_filtering_values(
            type_input_value,
            self.alerts.type_value_filtering,
            self.alerts.filtering_values,
        )
        validate_thresholds(
            type_input_value,
            self.alerts.type_value_threshold,
            self.alerts.threshold_min,
            self.alerts.threshold_max,
        )
        return self

    @staticmethod
    def options_for_unit_io(
        field: FieldDeclaration,
    ) -> DataPipeConfigOptionsRead:
        kind = FieldType(field[FieldKey.TYPE])
        modes = {
            TypeInputValue.TEXT: DataPipeModeOptions(
                policy_types=tuple(
                    item
                    for item in ProcessingPolicyType
                    if item != ProcessingPolicyType.AGGREGATION
                ),
                transformation_fields=TransformationConfig.TEXT_FIELDS,
            ),
        }
        numeric_range = None
        if kind in (FieldType.INT, FieldType.FLOAT):
            numeric_range = DataPipeNumericRange(
                min=field[FieldKey.MIN],
                max=field[FieldKey.MAX],
            )
            first_integer = ceil(numeric_range.min)
            last_integer = floor(numeric_range.max)
            threshold_types = ()
            if first_integer < last_integer:
                threshold_types = tuple(FilterTypeValueThreshold)
            elif first_integer == last_integer:
                threshold_types = (
                    FilterTypeValueThreshold.MIN,
                    FilterTypeValueThreshold.MAX,
                )
            modes[TypeInputValue.NUMBER] = DataPipeModeOptions(
                policy_types=tuple(ProcessingPolicyType),
                threshold_types=threshold_types,
                aggregation_functions=tuple(AggregationFunctions),
                transformation_fields=TransformationConfig.NUMBER_FIELDS,
            )
        return DataPipeConfigOptionsRead(
            value_type=kind,
            modes=modes,
            numeric_filter_range=numeric_range,
            time_window_sizes=settings.pu_time_window_sizes,
        )

    def check_unit_io(
        self, field: FieldDeclaration
    ) -> list[DataPipeValidationErrorRead]:
        options = self.options_for_unit_io(field)
        mode = options.modes.get(self.filters.type_input_value)
        threshold = self.filters.type_value_threshold
        issues = []
        if mode is None:
            issues.append(
                DataPipeValidationErrorRead(
                    stage=DataPipeStage.FILTERS,
                    message=f"Number mode requires int or float UnitIO. This UnitIO is {options.value_type.value}.",
                    error_code=ErrorCode.DATAPIPE_IO_TYPE_MISMATCH,
                )
            )
        elif (
            self.filters.type_input_value == TypeInputValue.NUMBER
            and options.numeric_filter_range is not None
        ):
            issues.extend(
                self.filters.check_numeric_limits(options.numeric_filter_range)
            )
        elif threshold is not None and threshold not in mode.threshold_types:
            issues.append(
                DataPipeValidationErrorRead(
                    stage=DataPipeStage.FILTERS,
                    message="type_value_threshold is not applicable to Text input",
                    error_code=ErrorCode.DATAPIPE_THRESHOLD_NOT_FOR_TEXT,
                )
            )

        policies = (mode or options.modes[TypeInputValue.TEXT]).policy_types
        if self.processing_policy.policy_type not in policies:
            issues.append(
                DataPipeValidationErrorRead(
                    stage=DataPipeStage.PROCESSING_POLICY,
                    message="Aggregation requires Number mode and an int or float UnitIO",
                    error_code=ErrorCode.DATAPIPE_IO_TYPE_MISMATCH,
                )
            )

        transformations = self.transformations
        if transformations is None:
            return issues
        allowed_fields = mode.transformation_fields if mode else ()
        invalid_fields = {
            name
            for name in DataPipeTransformationField
            if name not in allowed_fields
            and getattr(transformations, name) is not None
        }
        if invalid_fields.intersection(TransformationConfig.NUMBER_FIELDS):
            issues.append(
                DataPipeValidationErrorRead(
                    stage=DataPipeStage.TRANSFORMATIONS,
                    message="Multiplication and rounding require Number mode and an int or float UnitIO",
                    error_code=ErrorCode.DATAPIPE_IO_TYPE_MISMATCH,
                )
            )
        if invalid_fields.intersection(TransformationConfig.TEXT_FIELDS):
            issues.append(
                DataPipeValidationErrorRead(
                    stage=DataPipeStage.TRANSFORMATIONS,
                    message="Slicing requires Text mode",
                    error_code=ErrorCode.DATAPIPE_IO_TYPE_MISMATCH,
                )
            )
        return issues


def format_validation_error_dict(
    e: ValidationError,
) -> list[DataPipeValidationErrorRead]:
    # The only cross stage check is alerts against filters, it has no location
    return [
        DataPipeValidationErrorRead(
            stage=DataPipeStage(snake_to_camel(err["loc"][0]))
            if err["loc"]
            else DataPipeStage.ALERTS,
            message=err["msg"],
            error_code=int(
                ErrorCode.code_from_validation(
                    err, ErrorCode.DATAPIPE_CONFIG_INVALID
                )
            ),
        )
        for err in e.errors()
    ]


def is_valid_data_pipe_config(
    data: object,
    is_business_validator: bool = False,
    *,
    unit_io_field: FieldDeclaration | None = None,
) -> DataPipeConfig | list[DataPipeValidationErrorRead]:
    if data is None:
        msg = "DataPipe is None"
        raise DataPipeError(msg, code=ErrorCode.DATAPIPE_IS_NONE)

    try:
        config = DataPipeConfig.model_validate(data)
    except ValidationError as err:
        if not is_business_validator:
            return format_validation_error_dict(err)
        first = err.errors()[0]
        raise DataPipeError(
            first["msg"],
            code=ErrorCode.code_from_validation(
                first, ErrorCode.DATAPIPE_CONFIG_INVALID
            ),
        ) from err
    issues = (
        config.check_unit_io(unit_io_field)
        if unit_io_field is not None
        else []
    )
    if not is_business_validator:
        return issues
    if issues:
        first = issues[0]
        resolved = ErrorCode(first.error_code)
        raise DataPipeError(first.message, code=resolved)
    return config
