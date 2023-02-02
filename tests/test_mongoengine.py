import montydb
import mongoengine


def test_mongoengine_basic(monty_database):
    mongoengine.connect(db="test_db",
                        mongo_client_class=montydb.MontyClient,
                        repository=":memory:")

    class BlogPost(mongoengine.Document):
        title = mongoengine.StringField(required=True, max_length=200)
        meta = {'allow_inheritance': True}

    class TextPost(BlogPost):
        content = mongoengine.StringField(required=True)

    post1 = TextPost(title='Using MontyDB', content='Hello monty!')
    post1.save()

    assert TextPost.objects.count() == 1
