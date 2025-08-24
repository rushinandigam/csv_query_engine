import pandas as pd 
from projections.selectprojection import   QueryEngine
def main(): 
    file_path = input("Enter the address of the file : ")
    dataframe = pd.read_csv(file_path)
    file = QueryEngine(dataframe=dataframe)
    while True: 
        query = input("cql >")
        if(query == "exit"):
            break
        else : 
            print(file._execute_sql(query))


if __name__ == "__main__":
    main()
