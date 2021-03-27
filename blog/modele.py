from py2neo import Graph, Node, Relationship,NodeMatcher
from passlib.hash import bcrypt
from datetime import datetime
import os
import uuid
import pymongo as pm


# Connect to Mongo database
#mongo_local = pm.MongoClient('localhost', 27017)
#print(f"\n\n List of mongo databases : {mongo_local.list_database_names()}\n\n")


class MongoDb:
    def __init__(self, pymongo = None, uri= 'localhost', port = 27017):
        self.port = port
        self.uri = uri
        self.pymongo = pymongo
        self.connect_mongo()

    def connect_mongo(self):
        self.connection = self.pymongo.MongoClient(self.uri, self.port )
        print(f"\n\n List of mongo databases : {self.connection.list_database_names()}\n\n")

    def create_database(self, database_name = ""):
        # Creating an new mongo database
        self.database = self.connection[database_name]

        return self.database

    def create_collection(self, database_name = "" ,collection_name = ""):
        if not self.create_database(database_name =database_name):
            self.database = create_database( database_name = database_name)
        
        ## creating a collection
        self.collection = self.database[collection_name]
        return self.collection

mongo = MongoDb(pymongo = pm, uri= 'localhost', port = 27017)


graph = Graph("bolt://localhost:7687", user="keitaneo4j", password="keitaneo4j")

class User:
    def __init__(self, username = None , email =None, sex = None):
        self.username = username
        self.email=email
        self.sex=sex
        self.mongo_collection = mongo.create_collection(database_name = "mongo_blog" , collection_name = "User_blog")

    def find(self):
        matcher = NodeMatcher(graph)
        user = matcher.match("User", username=self.username).first()

        mongo_filter = {'username' : self.username }
        mongo_user = self.mongo_collection.find_one( mongo_filter )
        print(f"\n\n Mongo find user : { mongo_user } ")

        return user

    def register(self, password, email, sex):
        if not self.find():
            user = Node('User', username=self.username, password=bcrypt.encrypt(password), email=self.email, sex=self.sex)
            graph.create(user)

            input_dict = {'username' : self.username , 'password' : bcrypt.encrypt(password) , 'email' : self.email , "sex" : self.sex }
            self.mongo_collection.insert_one( input_dict )

            return True
        else:
            return False

    def verify_password(self, password ):
        user = self.find()
        if user:
            mongo_filter = {'username' : self.username }
            mongo_user = self.mongo_collection.find_one( mongo_filter )
            print(f"\n\n Mongo verify_password : { bcrypt.verify(password, mongo_user['password']) } ")

            return bcrypt.verify(password, user['password'])
        else:
            return False

    def add_post(self, title, tags, text):
        user = self.find()
        post = Node(
            'Post',
            id=str(uuid.uuid4()),
            title=title,
            text=text,
            timestamp=timestamp(),
            date=date()
        )
        rel = Relationship(user, 'PUBLISHED', post)
        graph.create(rel)

        new_post = { 'Post.id' : str(uuid.uuid4()) , 'Post.title': title, 'Post.tags': tags, 'Post.text': text , 'Post.timestamp': timestamp(), 'Post.date': date()}
        updated_user = {"$set": new_post  }

        mongo_filter = {'username' : self.username }
        mongo_user = self.mongo_collection.update_one( mongo_filter, updated_user)


        tags = [x.strip() for x in tags.lower().split(',')]
        for tag in set(tags):
            tag = Node('Tag', name=tag)
            graph.merge(tag,"Tag", "name")

            rel = Relationship(tag, 'TAGGED', post)
            graph.create(rel)

    def like_post(self, post_id):
        user = self.find()
        matcher = NodeMatcher(graph)
        post = matcher.match("Post", id=post_id).first()
        graph.merge(Relationship(user, 'LIKED', post))

    def get_recent_posts(self):
        query = '''
        MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
        WHERE user.username = $username
        RETURN post, COLLECT(tag.name) AS tags
        ORDER BY post.timestamp DESC LIMIT 5
        '''

        return graph.run(query, username=self.username)

    def get_similar_users(self):
        # Find three users who are most similar to the logged-in user
        # based on tags they've both blogged about.
        query = '''
        MATCH (you:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag:Tag),
              (they:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag)
        WHERE you.username = $username AND you <> they
        WITH they, COLLECT(DISTINCT tag.name) AS tags
        ORDER BY SIZE(tags) DESC LIMIT 3
        RETURN they.username AS similar_user, tags
        '''

        return graph.run(query, username=self.username)

    def get_commonality_of_user(self, other):
        # Find how many of the logged-in user's posts the other user
        # has liked and which tags they've both blogged about.
        query = '''
        MATCH (they:User {username: $they })
        MATCH (you:User {username: $you })
        OPTIONAL MATCH (they)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag:Tag),
                       (you)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag)
        RETURN SIZE((they)-[:LIKED]->(:Post)<-[:PUBLISHED]-(you)) AS likes,
               COLLECT(DISTINCT tag.name) AS tags
        '''

        return graph.run(query, they=other.username, you=self.username).next

def get_todays_recent_posts():
    query = '''
    MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
    WHERE post.date = $today
    RETURN user.username AS username, post, COLLECT(tag.name) AS tags
    ORDER BY post.timestamp DESC LIMIT 5
    '''

    return graph.run(query, today=date())

def timestamp():
    epoch = datetime.utcfromtimestamp(0)
    now = datetime.now()
    delta = now - epoch
    return delta.total_seconds()

def date():
    return datetime.now().strftime('%Y-%m-%d')


