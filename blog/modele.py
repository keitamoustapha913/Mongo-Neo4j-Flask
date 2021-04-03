from py2neo import Graph, Node, Relationship,NodeMatcher
from passlib.hash import bcrypt
from datetime import datetime
import os
import uuid
import pymongo as pm


# Connect to Mongo database
#mongo_local = pm.MongoClient('localhost', 27017)
#print(f"\n\n List of mongo databases : {mongo_local.list_database_names()}\n\n")
#mongodb+srv://BelkacemBerbache:Kamax5477@cluster0.fmtsc.mongodb.net/myFirstDatabase?retryWrites=true&w=majority
########################### Class MongoDb ###############################
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

#################### Flask Blog - Class User - Neo4j ##########################
graph = Graph("bolt://localhost:7687", user="keitaneo4j", password="keitaneo4j")


class User:
    def __init__(self, username = None , email =None, sex = None):
        self.username = username
        self.email=email
        self.sex=sex
        self.user_collection = mongo.create_collection(database_name = "mongo_blog" , collection_name = "User_blog")
        self.post_collection = mongo.create_collection(database_name = "mongo_blog" , collection_name = "Post_blog")
        self.tag_collection = mongo.create_collection(database_name = "mongo_blog" , collection_name = "Tag_blog")
        self.like_collection = mongo.create_collection(database_name = "mongo_blog" , collection_name = "Like_blog")

    def find(self):
        matcher = NodeMatcher(graph)
        user = matcher.match("User", username=self.username).first()

        mongo_filter = {'username' : self.username }
        mongo_user = self.user_collection.find_one( mongo_filter )
        print(f"\n\n Mongo find user : { mongo_user } ")

        return user

    def register(self, password, email, sex):
        if not self.find():
            user = Node('User', username=self.username, password=bcrypt.encrypt(password), email=self.email, sex=self.sex)
            graph.create(user)

            input_dict = {'username' : self.username , 'password' : bcrypt.encrypt(password) , 'email' : self.email , "sex" : self.sex, "Post":[] }
            self.user_collection.insert_one( input_dict )

            return True
        else:
            return False

    def verify_password(self, password ):
        user = self.find()
        if user:
            mongo_filter = {'username' : self.username }
            mongo_user = self.user_collection.find_one( mongo_filter )
            print(f"\n\n Mongo verify_password : { bcrypt.verify(password, mongo_user['password']) } ")

            return bcrypt.verify(password, user['password'])
        else:
            return False

    def add_post(self, title, tags, text):
        user = self.find()
        post_id = str(uuid.uuid4())
        post = Node(
            'Post',
            id= post_id ,
            title=title,
            text=text,
            timestamp=timestamp(),
            date=date()
        )
        rel = Relationship(user, 'PUBLISHED', post)
        graph.create(rel)

        mongo_filter = {'username' : self.username }
        mongo_user = self.user_collection.find_one( mongo_filter )

        



        tags = [x.strip() for x in tags.lower().split(',')]
        
        insert_post = { '_id' : post_id ,"User_blog":mongo_user["_id"] , 'title': title, 'text': text , 'timestamp': timestamp(), 'date': date(), "tags":tags, "likes":[]}

        self.post_collection.insert_one( insert_post)

       

        #print( f"all users : {users_tagged } \n")
        #print( f"\nall users dict : {tags_dict } \n")

        for tag in set(tags):
            """ 
            tag_filter = {'name' : tag }
            if self.tag_collection.find_one( tag_filter ) is not None:
                updated_tag = {"$addToSet":{"User_blog": mongo_user["_id"]}}

                #user_filter = { "User_blog": { "$all": [mongo_user["_id"]] } }
                #found_user = self.tag_collection.find_one( user_filter )
                #print(f"found user id tag: {found_user}")

                #if found_user is None:
                self.tag_collection.update( tag_filter, updated_tag)

            else:
                insert_tag = { '_id' :str(uuid.uuid4()) , "name":tag ,"User_blog":[mongo_user["_id"]]  }
                self.tag_collection.insert_one( insert_tag )
            """


            tag = Node('Tag', name=tag)
            graph.merge(tag,"Tag", "name")

            rel = Relationship(tag, 'TAGGED', post)
            graph.create(rel)


    def like_post(self, post_id):
        user = self.find()
        print(f"user like self : {self.username}")
        print(f"user neo4j : {user}")

        mongo_filter = {'username' : self.username }
        mongo_user = self.user_collection.find_one( mongo_filter )

        updated_likes = {"$addToSet":{"likes": self.username }}
        post_filter = { "_id" : post_id }
        print(f"Post id : {post_id}")
        self.post_collection.update( post_filter , updated_likes)

        mongo_post = self.post_collection.find_one( post_filter )
        print(f"Post liked : {mongo_post}")


        matcher = NodeMatcher(graph)
        post = matcher.match("Post", id=post_id).first()
        graph.merge(Relationship(user, 'LIKED', post))
        
        """

        if self.like_collection.find_one( like_filter ) is not None:
            like_update = {"$addToSet":{"User_blog": mongo_user["_id"]}}

            self.tag_collection.update( like_filter , like_update)

        else:
            insert_tag = { '_id' :str(uuid.uuid4()) , "name":tag ,"User_blog":[mongo_user["_id"]]  }
            self.tag_collection.insert_one( insert_tag )


        self.like_collection.insert_one(like_filter)
        """

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
        mongo_filter = {'username' : self.username }
        mongo_user = self.user_collection.find_one( mongo_filter )

        post_filter= {"User_blog" : mongo_user["_id"]}
        mongo_post = self.post_collection.find( post_filter )
        user_tags_distinct = mongo_post.distinct("tags")
        print(f"Found tags : {user_tags_distinct}")
        users_tagged = []
        tags_dict = {}
        for user_tag in user_tags_distinct:

            tag_filter = { "tags": user_tag }
            mongo_tags = self.post_collection.find( tag_filter )
            distinct_users = mongo_tags.distinct( "User_blog" )
            user_name_list = []
            for distinct_user in distinct_users:
                user = self.user_collection.find_one( {"_id": distinct_user} )
                if (user is not None) and (user["username"] != self.username):
                    user_name_list.append( user["username"] )
            tags_dict[user_tag] = user_name_list
        
        print( f"\nall users dict : {tags_dict } \n")

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
