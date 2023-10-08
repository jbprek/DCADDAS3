# Join Types
## Inner Join
The inner join is the default join in Spark SQL. It selects rows that have matching values in both relations.

Syntax:

relation [ INNER ] JOIN relation [ join_criteria ]

## Left Join
A left join returns all values from the left relation and the matched values from the right relation, or appends NULL if there is no match. It is also referred to as a left outer join.

Syntax:

relation LEFT [ OUTER ] JOIN relation [ join_criteria ]

##  Right Join
A right join returns all values from the right relation and the matched values from the left relation, or appends NULL if there is no match. It is also referred to as a right outer join.

Syntax:

relation RIGHT [ OUTER ] JOIN relation [ join_criteria ]

##  Full Join
A full join returns all values from both relations, appending NULL values on the side that does not have a match. It is also referred to as a full outer join.

Syntax:

relation FULL [ OUTER ] JOIN relation [ join_criteria ]

##  Cross Join
A cross join returns the Cartesian product of two relations.

Syntax:

relation CROSS JOIN relation [ join_criteria ]

## Semi Join
A semi join returns values from the left side of the relation that has a match with the right. It is also referred to as a left semi join.

Syntax:

relation [ LEFT ] SEMI JOIN relation [ join_criteria ]

##  Anti Join
An anti join returns values from the left relation that has no match with the right. It is also referred to as a left anti join.

Syntax:

relation [ LEFT ] ANTI JOIN relation [ join_criteria ]