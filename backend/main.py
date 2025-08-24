import pandas as pd 
from projections.selectprojection import QueryEngine

def main(): 
    file_path = input("Enter the address of the file : ")
    dataframe = pd.read_csv(file_path)
    
    while True: 
        query = input("cql >")
        if(query == "exit"):
            break
        


if __name__ == "__main__":
    main()
