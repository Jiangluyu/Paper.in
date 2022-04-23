from django.db import models
from django.contrib.postgres.fields import ArrayField


# Create your models here.
class Authors(models.Model):
    author_id = models.IntegerField(primary_key=True, null=False)
    author_name = models.CharField(max_length=255, null=False)
    affiliations = ArrayField(
        models.TextField(max_length=255), null=True
    )
    firstname = models.CharField(max_length=255, null=True)
    lastname = models.CharField(max_length=255, null=True)


class Paper(models.Model):
    paper_doi = models.TextField(primary_key=True, null=False)
    title = models.TextField(null=False)
    publishedby = models.TextField(null=False)
    author_id = ArrayField(
        models.IntegerField(), null=False
    )
    ieee_keywords = ArrayField(
        models.TextField(), null=True
    )
    authors_keywords = ArrayField(
        models.TextField(), null=True
    )
    pubTopics = ArrayField(
        models.TextField(), null=True
    )
    publicationNumber = models.CharField(max_length=255, null=False)
    publicationDate = models.IntegerField(null=True)
    metrics = ArrayField(
        models.IntegerField(), null=False
    )
    refs = ArrayField(
        models.TextField(), null=True
    )
    googleScholarLink = ArrayField(
        models.TextField(), null=True
    )
    refs_doi = ArrayField(
        models.TextField(), null=True
    )
