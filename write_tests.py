types = ["smallint", "integer", "bigint", "numeric", "real"]

concat = False
prepend = True
append = False

for outer in types: 
    for inner in types:
        if concat: 
            # both arrays
            print("query T")
            print(f"select array[1]::{inner}[] || array[1]::{outer}[];")
            print("----")
            print(r"{1, 1}")
            print()

            # left scalar right array
            print("query T")
            print(f"select 1::{inner} || array[1]::{outer}[];")
            print("----")
            print(r"{1, 1}")
            print()
        
            # left array right scalar
            print("query T")
            print(f"select array[1]::{inner}[] || 1::{outer};")
            print("----")
            print(r"{1, 1}")
            print()
        
        if prepend: 
            # left scalar right array
            print("query T")
            print(f"select array_prepend(1::{inner}, array[1]::{outer}[]);")
            print("----")
            print(r"{1, 1}")
            print()
        
        if append:
            # left array right scalar
            print("query T")
            print(f"select array_append(array[1]::{inner}[], 1::{outer});")
            print("----")
            print(r"{1, 1}")
            print()        
        

