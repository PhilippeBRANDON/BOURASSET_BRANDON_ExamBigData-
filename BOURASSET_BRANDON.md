# Examen CC2 _ Mise en pratique (Examen)
#### Philippe BRANDON & Aël BOURASSET
#

##### *En partant du fichier ml-25m/tags.csv du TP Hadoop, il nous est demandé de répondre à ces questions :* https://files.grouplens.org/datasets/movielens/ml-25m.zip

## Mise en place de l'environnement Haddop sous la distribution SandBowx HDP en VM 

Pour mettre en place l'environnement Haddop nous avons utilisé la commande :

    ssh maria_dev@localhost -p 2222
Et le mot de passe est :

    maria_dev


## Installation Python et Packages 
Pour pouvoir réaliser l'Exam CC2 on a du installer pip. Pour cela nous avons utilisé les commandes suivantes car les outils "automatiques" sont obsolètes : 

    curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py

    sudo python get-pip.py


Par la suite nous avons procéder a l'installation des *packages*  dans l'invite de commandes :


    pip install pathlib
    pip install mrjob==0.7.4
    pip install PyYAML==5.4.1


Pour pouvoir intégrer les fichier dans HDFS nous les avons récupérer a l'aide de la commande : 

    wget https://files.groupelens.org/datasets/movielens/ml25m.zip" 

Pour répondre a ces questions, nous avons décidé de créer sur git hub les fichier .py répondants auw questions et par la suite nous les avons téléchargés sur notre machine virtuel à l'aide d'un "*wget*" pour y déposer notre script et notre tags_sample. Par la suite nous les testons en local avant de les envoyer dans HDFS :

#### Code utilisé :

On déplace le fichier tags.csv vers hdfs : 
    
    hdfs dfs -put ml-25m/tags.csv

Pour vérifier que notre commande a bien fonctionné et que le déplacement à bien été fait nous avons utilisé la commande suivante : 

    dfs -ls /user/maria_dev/

Ce qui nous a sorti la liste des fichier dans maria_dev, dont *tags.csv*.

Voici le code générique utilisé pour appliquer le MapReduce au fichier :

    python [lien vers votre fichier python] -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar [lien vers votre fichier des données sur hdfs] -o [lien vers le fichier output des résultats] <-- dossier >



#### 1. Combien de tags chaque film possède-t-il ?

Script : 
```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieTagCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_get_movie_tags(self, _, line):
        try:
            if line.startswith("userId"):
                return
            fields = line.strip().split(',')
            if len(fields) >= 3:
                movieId = fields[1]  
                yield movieId, 1
        except Exception as e:
            self.stderr.write("Erreur dans le mapper: {}\n".format(e))

    def reducer_count_tags(self, movieId, counts):
        yield movieId, sum(counts)

if __name__ == '__main__':
    MovieTagCount.run()
```
Commande pour ressortir les 10 premiers résultats :

    hdfs dfs -cat /user/maria_dev/output1/part-OOOOO | head
    
Extrait des résultats:
```python
"1"         697
"10"        137
"100"       18
"1000"      10
"100001"    1
"100003"    3
"100008"    9
"100017"    9
"100032"    2
"100034"    19

```

Commande pour exécuter le code sur hdfs et stocker le résultat :

    python Reponse1.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags.csv -o output1

On renomme ensuite le fichier de résultats :

    hadoop fs -mv hdfs:///user/maria_dev/output1/part-00000 hdfs:///user/maria_dev/output/rep1.txt


#### 2. Combien de tags chaque utilisateur a-t-il ajoutés ?
```python
    from mrjob.job import MRJob
    from mrjob.step import MRStep

    class UserTagCount(MRJob):

        def steps(self):
            return [
                MRStep(mapper=self.mapper_get_user_tags,
                    reducer=self.reducer_count_tags)
            ]

        def mapper_get_user_tags(self, _, line):
            try:
                if line.startswith("userId"):
                    return
                fields = line.strip().split(',')
            
                userId = fields[0]
                yield userId, 1
            except Exception as e:
                self.stderr.write(f"Erreur dans le mapper: {e}\n")

        def reducer_count_tags(self, userId, counts):
            yield userId, sum(counts)

    if __name__ == '__main__':
        UserTagCount.run()
```

Commande pour ressortir les 10 premiers résultats :

    hdfs dfs -cat /user/maria_dev/output2/part-OOOOO | head

Extrait de l'echantillon : 
```python
    "100001"        9
    "100016"        50
    "100028"        4
    "100029"        1
    "100033"        1
    "100046"        133
    "100051"        19
    "100058"        5
    "100065"        2
    "100068"        19
```

Commande pour exécuter le code sur hdfs et stocker le résultat :

    python Reponse2.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar hdfs:///user/maria_dev/tags.csv -o output2

On renomme ensuite le fichier de résultats :

    hadoop fs -mv hdfs:///user/maria_dev/output2/part-00000 hdfs:///user/maria_dev/output/rep2.txt



## Avec la configuration de Hadoop suivante (taille du bloc par défaut et taille du bloc = 64 Mo)

##### *Pour la question 3 on va "put" cependant nous allons modifier la taille du bloc* 

     hadoop fs -D dfs.block.size=67108864 -put ml-25m/tags.csv /user/maria_dev/tags2.csv

#### 3. Combien de blocs le fichier occupe-t-il dans HDFS dans chacune des configurations ?

Commande :

    hdfs fsck tags.csv -files -blocks -locations

Résultat :

    [maria_dev@sandbox-hdp ~]$ hdfs fsck tags.csv -files -blocks -locations
    Connecting to namenode via http://sandbox-hdp.hortonworks.com:50070/fsck?ugi=maria_dev&files=1&blocks=1&locations=1&path=%2Fuser%2Fmaria_dev%2Ftags.csv
    FSCK started by maria_dev (auth:SIMPLE) from /172.18.0.2 for path /user/maria_dev/tags.csv at Thu Apr 09 13:40:02 UTC 2026
    /user/maria_dev/tags.csv 38810332 bytes, 1 block(s):  OK
    0. BP-243674277-172.17.0.2-1529333510191:blk_1073743160_2342 len=38810332 repl=1 [DatanodeInfoWithStorage[172.18.0.2:50010,DS-ab75b94d-c6f2-4415-8639-1aaec2609e13,DISK]]

        Status: HEALTHY
        Total size:    38810332 B
        Total dirs:    0
        Total files:   1
        Total symlinks:                0
        Total blocks (validated):      1 (avg. block size 38810332 B)
        Minimally replicated blocks:   1 (100.0 %)
        Over-replicated blocks:        0 (0.0 %)
        Under-replicated blocks:       0 (0.0 %)
        Mis-replicated blocks:         0 (0.0 %)
        Default replication factor:    1
        Average block replication:     1.0
        Corrupt blocks:                0
        Missing replicas:              0 (0.0 %)
        Number of data-nodes:          1
        Number of racks:               1
        FSCK ended at Thu Apr 09 13:40:02 UTC 2026 in 7 milliseconds


    The filesystem under path '/user/maria_dev/tags.csv' is HEALTHY


Commande :

        hdfs fsck tags2.csv -files -blocks -locations

Résultat :

    Status: HEALTHY
    Total size:    38810332 B
    Total dirs:    0
    Total files:   1
    Total symlinks:                0
    Total blocks (validated):      1 (avg. block size 38810332 B)
    Minimally replicated blocks:   1 (100.0 %)
    Over-replicated blocks:        0 (0.0 %)
    Under-replicated blocks:       0 (0.0 %)
    Mis-replicated blocks:         0 (0.0 %)
    Default replication factor:    1
    Average block replication:     1.0
    Corrupt blocks:                0
    Missing replicas:              0 (0.0 %)
    Number of data-nodes:          1
    Number of racks:               1
    FSCK ended at Thu Apr 09 13:41:02 UTC 2026 in 2 milliseconds

Le fichier ne fait que 38Mo donc le nombre de blocs ne changera pas, tant qu'il ne fait pas plus de 128Mo dans le premier cas et 64Mo dans le second, il n'y aura qu'1 seul bloc.


#### 4. Combien de fois chaque tag a-t-il été utilisé pour taguer un film ? 

```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class TagFrequencyCounter(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_tags,
                   reducer=self.reducer_count_tags)
        ]

    def mapper_extract_tags(self, _, line):
        # Ignore l'en-tête
        if line.startswith("userId"):
            return
        try:
            userId, movieId, tag, timestamp = line.strip().split(',', 3)
            yield tag.lower().strip(), 1  # tag en minuscules et nettoyé
        except ValueError:
            pass  # Ignore les lignes mal formées

    def reducer_count_tags(self, tag, counts):
        yield tag, sum(counts)

if __name__ == '__main__':
    TagFrequencyCounter.run()
```

Commande pour exécuter le code sur hdfs et stocker le résultat :

    python Reponse4.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags.csv \
    -o output4

Commande pour ressortir les 10 premiers résultats :

    hdfs dfs -cat /user/maria_dev/output4/part-OOOOO | head

Résultats : 

    "!950's superman tv show"       1
    "#1 prediction" 3
    "#adventure"    1
    "#antichrist"   1
    "#boring #lukeiamyourfather"    1
    "#boring"       1
    "#danish"       2
    "#documentary"  1
    "#entertaining" 1
    "#exorcism"     1

On renomme ensuite le fichier de résultats :

    hadoop fs -mv hdfs:///user/maria_dev/output4/part-00000 hdfs:///user/maria_dev/output/rep4.txt

### 5. Pour chaque film, combien de tags le même utilisateur a-t-il introduits ?
```python
from mrjob.job import MRJob
from mrjob.step import MRStep

class UserMovieTagCount(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_extract_user_movie,
                reducer=self.reducer_count_tags_per_user_movie)
        ]

    def mapper_extract_user_movie(self, _, line):
        if line.startswith("userId"):
            return
        try:
            userId, movieId, tag, timestamp = line.strip().split(',', 3)
            key = (userId, movieId)
            yield key, 1
        except ValueError:
            pass  # ignore les lignes malformées

    def reducer_count_tags_per_user_movie(self, user_movie, counts):
        yield user_movie, sum(counts)

if __name__ == '__main__':
    UserMovieTagCount.run()
```

Commande pour exécuter le code sur hdfs et stocker le résultat :

    python Reponse5.py -r hadoop \
    --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
    hdfs:///user/maria_dev/tags.csv \
    -o output5

Commande pour ressortir les 10 premiers résultats :

    hdfs dfs -cat /user/maria_dev/output5/part-OOOOO | head

Résultats : 

    ["100001", "260"]       9
    ["100016", "111659"]    3
    ["100016", "112290"]    3
    ["100016", "114180"]    4
    ["100016", "117438"]    2
    ["100016", "1391"]      4
    ["100016", "1584"]      5
    ["100016", "2012"]      2
    ["100016", "377"]       4
    ["100016", "43560"]     1

On renomme ensuite le fichier de résultats :

    hadoop fs -mv hdfs:///user/maria_dev/output5/part-00000 hdfs:///user/maria_dev/output/rep5.txt


L'ensemble des codes pythons et les résultats sont disponible sur ce Git :

    https://github.com/PhilippeBRANDON/BOURASSET_BRANDON_ExamBigData-/tree/main
    