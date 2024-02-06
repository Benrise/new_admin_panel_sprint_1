import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField('name', max_length=255)
    description = models.TextField('description', blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = 'Жанр'
        verbose_name_plural = 'Жанры'

    def __str__(self):
        return self.name 


class Filmwork(models.Model):
    class Type(models.TextChoices):
        MOVIE = "movie"
        TV_SHOW = "tv_show"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField('name', max_length=255)
    description = models.TextField('description', blank=True)
    creation_date = models.DateTimeField(auto_now_add=True)
    rating = models.FloatField(default=0.0,
                               validators=[MinValueValidator(0.0),
                                           MaxValueValidator(10)],
                               blank=True)
    type = models.CharField(max_length=255, choices=Type.choices)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    class Meta:
        db_table = "content\".\"filmwork"
        verbose_name = 'Кинопроизведение'
        verbose_name_plural = 'Кинопроизведения'

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    filmwork = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_filmwork"


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField('name', max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = 'Персонаж'
        verbose_name_plural = 'Персонажи'

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):
    filmwork = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.TextField('role')
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_filmwork"
