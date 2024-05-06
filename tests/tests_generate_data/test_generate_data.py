from src.gen_sample_data.generate_data import load_us_population_data


def test_load_us_population_data():
    # Arrange
    us_population_data = load_us_population_data()
    # got 7 columns
    assert len(us_population_data) == 7
    # got 51 states
    assert len(us_population_data["State"]) == 51
    # first state is alabama
    assert us_population_data["State"][0] == "Alabama"
