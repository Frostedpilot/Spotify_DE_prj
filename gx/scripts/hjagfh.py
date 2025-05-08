import great_expectations as gx

context = gx.get_context()

# All Expectations are found in the `gx.expectations` module.
# This Expectation has all values set in advance:
preset_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count", min_value=1, max_value=6
)

print(preset_expectation)

# In this case, two Expectations are created that will be passed
#  parameters at runtime, and unique lookups are defined for each
#  Expectations' parameters.

passenger_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="passenger_count",
    min_value={"$PARAMETER": "expect_passenger_max_to_be_above"},
    max_value={"$PARAMETER": "expect_passenger_max_to_be_below"},
)
fare_expectation = gx.expectations.ExpectColumnMaxToBeBetween(
    column="fare",
    min_value={"$PARAMETER": "expect_fare_max_to_be_above"},
    max_value={"$PARAMETER": "expect_fare_max_to_be_below"},
)

# A dictionary containing the parameters for both of the above
#   Expectations would look like:
runtime_expectation_parameters = {
    "expect_passenger_max_to_be_above": 4,
    "expect_passenger_max_to_be_below": 6,
    "expect_fare_max_to_be_above": 10.00,
    "expect_fare_max_to_be_below": 500.00,
}
