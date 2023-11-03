from lib import init_spark, load_csv, summary, analysis

def main():
    data_path = "https://github.com/mwaskom/seaborn-data/raw/master/penguins.csv"
    
    data = lib.load_csv(data_path)

    print("\nData Summary:")
    lib.summary(data)

    print("\nSpecies Analysis:")
    lib.analysis(data)

if __name__ == "__main__":
    main()
