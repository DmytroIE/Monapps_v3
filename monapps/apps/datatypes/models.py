from django.db import models


from common.constants import VariableTypes, DataAggrTypes


class DataType(models.Model):

    class Meta:
        db_table = "datatypes"

    name = models.CharField(default="Dimensionless")
    meas_unit = models.CharField(default="", blank=True)
    agg_type = models.IntegerField(default=DataAggrTypes.AVG, choices=DataAggrTypes.choices)
    var_type = models.IntegerField(default=VariableTypes.CONTINUOUS, choices=VariableTypes.choices)

    def __str__(self):
        return f"Datatype {self.name}"
