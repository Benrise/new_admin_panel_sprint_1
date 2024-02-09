import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(_('created'), auto_now_add=True)
    modified = models.DateTimeField(_('modified'), auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = _('genre')
        verbose_name_plural = _('genres')

    def __str__(self):
        return self.name


class Filmwork(UUIDMixin):
    class Type(models.TextChoices):
        MOVIE = "movie", _('movie')
        TV_SHOW = "tv_show", _('tv_show')

    title = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateTimeField(_('creation_date'), auto_now_add=True)
    rating = models.FloatField(_('rating'),
                               default=0.0,
                               validators=[MinValueValidator(0.0),
                                           MaxValueValidator(10)],
                               blank=True)
    type = models.CharField(_('type'), max_length=255, choices=Type.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = _('film_work')
        verbose_name_plural = _('film_works')

    def __str__(self):
        return self.title


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE,
                                  verbose_name=_('film_work'))
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE,
                              verbose_name=_('person'))
    created = models.DateTimeField(_('created'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"genre_film_work"
        verbose_name = _('genre_film_work')
        verbose_name_plural = _('genre_film_works')


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('fullname'), max_length=255)

    class Meta:
        db_table = "content\".\"person"
        verbose_name = _('person')
        verbose_name_plural = _('persons')

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE,
                                  verbose_name=_('film_work'))
    person = models.ForeignKey('Person', on_delete=models.CASCADE,
                               verbose_name=_('person'))
    role = models.TextField(_('role'))
    created = models.DateTimeField(_('created'), auto_now_add=True)

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = _('person_film_work')
        verbose_name_plural = _('person_film_works')
