sub_q = ["(array[1])[1]", "1"]
ops = [">", "<", "<=", ">=", "=", "!="]

for op in ops:
    print("query T")
    print(
        f"select ({sub_q[0]} {op} {sub_q[1]}) = ({sub_q[1]} {op} {sub_q[0]});")
    print("----")
    print("t")
    print("")
