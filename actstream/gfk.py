from django.conf import settings
from django.db.models import Manager
from django.db.models.query import QuerySet, EmptyQuerySet

from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.generic import GenericForeignKey

from batch_select.models import *
from batch_select.models import _check_field_exists, _id_attr, _not_exists, _select_related_instances

USE_PREFETCH = getattr(settings, 'USE_PREFETCH', False)
FETCH_RELATIONS = getattr(settings, 'FETCH_RELATIONS', True)


class GFKManager(Manager):
    """
    A manager that returns a GFKQuerySet instead of a regular QuerySet.

    """
    def get_query_set(self):
        return GFKQuerySet(self.model, using=self.db)

    def none(self):
        return self.get_query_set().none()


class GFKQuerySet(QuerySet):
    """
    A QuerySet with a fetch_generic_relations() method to bulk fetch
    all generic related items.  Similar to select_related(), but for
    generic foreign keys.

    Based on http://www.djangosnippets.org/snippets/984/
    Firstly improved at http://www.djangosnippets.org/snippets/1079/

    Extended in django-activity-stream to allow for multi db, text primary keys
    and empty querysets.
    """
    def _clone(self, *args, **kwargs):
        query = super(GFKQuerySet, self)._clone(*args, **kwargs)
        batches = getattr(self, '_batches', None)
        if batches:
            query._batches = set(batches)
        return query

    def _create_batch(self, batch_or_str, target_field_name=None):
        batch = batch_or_str
        if isinstance(batch_or_str, basestring):
            batch = Batch(batch_or_str)
        if target_field_name:
            batch.target_field_name = target_field_name

        _check_field_exists(self.model, batch.m2m_fieldname)
        return batch

    def batch_select(self, *batches, **named_batches):
        batches = getattr(self, '_batches', set()) | \
                  set(self._create_batch(batch) for batch in batches) | \
                  set(self._create_batch(batch, target_field_name) \
                        for target_field_name, batch in named_batches.items())

        query = self._clone()
        query._batches = batches
        return query

    def iterator(self):
        result_iter = super(GFKQuerySet, self).iterator()
        batches = getattr(self, '_batches', None)
        if batches:
            results = list(result_iter)
            for batch in batches:
                results = batch_select(self.model, results,
                                       batch.target_field_name,
                                       batch.m2m_fieldname,
                                       batch.replay)
            return iter(results)
        return result_iter

    def fetch_generic_relations(self, *args):
        qs = self._clone()

        if not FETCH_RELATIONS:
            return qs

        gfk_fields = [g for g in self.model._meta.virtual_fields
                      if isinstance(g, GenericForeignKey)]
        if args:
            gfk_fields = filter(lambda g: g.name in args, gfk_fields)

        if USE_PREFETCH and hasattr(self, 'prefetch_related'):
            return qs.prefetch_related(*[g.name for g in gfk_fields])

        ct_map, data_map = {}, {}

        for item in qs:
            for gfk in gfk_fields:
                ct_id_field = self.model._meta.get_field(gfk.ct_field).column
                ct_map.setdefault(getattr(item, ct_id_field), {}
                    )[smart_unicode(getattr(item, gfk.fk_field))] = (gfk.name,
                        item.pk)

        ctypes = ContentType.objects.using(self.db).in_bulk(ct_map.keys())

        for ct_id, items_ in ct_map.items():
            if ct_id:
                ct = ctypes[ct_id]
                model_class = ct.model_class()
                if hasattr(model_class._default_manager, 'all_with_deleted'):
                    objects = model_class._default_manager.all_with_deleted().select_related()
                else:
                    objects = model_class.objects.select_related()
                for o in objects.filter(pk__in=items_.keys()):
                    (gfk_name, item_id) = items_[o.pk]
                    data_map[(ct_id, o.pk)] = o

        for item in qs:
            for gfk in gfk_fields:
                if getattr(item, gfk.fk_field) != None:
                    ct_id_field = self.model._meta.get_field(gfk.ct_field)\
                        .column
                    setattr(item, gfk.name,
                        data_map[(
                            getattr(item, ct_id_field),
                            smart_unicode(getattr(item, gfk.fk_field))
                        )])

        return qs

    def none(self):
        return self._clone(klass=EmptyGFKQuerySet)


class EmptyGFKQuerySet(GFKQuerySet, EmptyQuerySet):
    def fetch_generic_relations(self):
        return self
