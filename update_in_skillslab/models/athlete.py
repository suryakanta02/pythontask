class AthleteTest:
    """Model class representing an athlete test record."""
    
    def __init__(self, athlete_id, name, age, test_date, test_type, 
                 test_result, coach_comments):
        self.athlete_id = athlete_id
        self.name = name
        self.age = age
        self.test_date = test_date
        self.test_type = test_type
        self.test_result = test_result
        self.coach_comments = coach_comments
    
    @classmethod
    def from_dict(cls, data_dict):
        """Create an AthleteTest instance from a dictionary."""
        return cls(
            athlete_id=data_dict['athlete_id'],
            name=data_dict['name'],
            age=data_dict['age'],
            test_date=data_dict['test_date'],
            test_type=data_dict['test_type'],
            test_result=data_dict['test_result'],
            coach_comments=data_dict['coach_comments']
        )
    
    def to_tuple(self):
        """Convert the AthleteTest instance to a tuple for database insertion."""
        return (
            self.athlete_id,
            self.name,
            self.age,
            self.test_date,
            self.test_type,
            self.test_result,
            self.coach_comments
        )