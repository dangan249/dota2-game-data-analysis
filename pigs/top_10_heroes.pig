DEFINE	CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

RECORDS = load '/Users/andang/IdeaProjects/test_result/project/part-r-00000' USING CSVLoader AS
          (heroId:int, itemSet:chararray, kills:int, deaths:int);

GROUPED_BY_HERO_ID = GROUP RECORDS BY heroId;
HERO_COUNTS = FOREACH GROUPED_BY_HERO_ID GENERATE group, COUNT(RECORDS) as heroCount;
SORTED_HERO_COUNTS = ORDER HERO_COUNTS BY heroCount DESC ;
SORTED_HERO_COUNTS_TOP_10 = LIMIT SORTED_HERO_COUNTS 10;
TOP_10_HEROES = FOREACH SORTED_HERO_COUNTS_TOP_10 GENERATE group;
store TOP_10_HEROES into '/Users/andang/IdeaProjects/test_result/project_pig/';
