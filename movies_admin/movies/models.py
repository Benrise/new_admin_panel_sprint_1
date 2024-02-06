import uuid
from django.db import models
from django.core.validators import MinValueValidator


class Genre(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField('name', max_length=255)
    description = models.TextField('description', blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'


class Filmwork(models.Model):
    class Type(models.TextChoices):
        MOVIE = "movie"
        TV_SHOW = "tv_show"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField('name', max_length=255)
    description = models.TextField('description', blank=True)
    creation_date = models.DateTimeField(auto_now_add=True)
    rating = models.FloatField(default=0.0,
                               validators=[MinValueValidator(0.0)],
                               blank=True)
    type = models.CharField(max_length=255, choices=Type.choices)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "content\".\"filmwork"
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведения'
