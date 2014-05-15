# -*- coding: utf-8 -*-

"""
django.db.models.Modelを拡張して、PostgreSQLのtsvector型に対応したモデルとマネジャを提供します。
http://djangosnippets.org/snippets/1328/
で公開されているコードを一部修正して使わせていただきました。
"""

from django.db import models


class VectorField (models.Field):

    def __init__( self, *args, **kwargs ):
        kwargs['null'] = True
        kwargs['editable'] = False
        kwargs['serialize'] = False
        super( VectorField, self ).__init__( *args, **kwargs )

    def db_type( self, connection=None ):
        return 'tsvector'


class SearchableModel (models.Model):

    search_index = VectorField()

    class Meta:
        abstract = True

    def update_index( self ):
        if hasattr( self, '_search_manager' ):
            self._search_manager.update_index( pk=self.pk )

    def save( self, *args, **kwargs ):
        super( SearchableModel, self ).save( *args, **kwargs )
        if hasattr( self, '_auto_reindex' ):
            if self._auto_reindex:
                self.update_index()
        else:
            self.update_index()


class SearchManager (models.Manager):

    def __init__( self, fields=None, config=None ):
        self.fields = fields
        self.default_weight = 'A'
        self.config = config and config or 'pg_catalog.english'
        self._vector_field_cache = None
        super( SearchManager, self ).__init__()

    def contribute_to_class( self, cls, name ):
        setattr( cls, '_search_manager', self )
        super( SearchManager, self ).contribute_to_class( cls, name )

    def _find_text_fields( self ):
        fields = [f for f in self.model._meta.fields if isinstance(f,(models.CharField,models.TextField))]
        return [f.name for f in fields]

    def _vector_field( self ):
        if self._vector_field_cache is not None:
            return self._vector_field_cache
        vectors = [f for f in self.model._meta.fields if isinstance(f,VectorField)]
        if len(vectors) != 1:
            raise ValueError( "There must be exactly 1 VectorField defined for the %s model." % self.model._meta.object_name )
        self._vector_field_cache = vectors[0]
        return self._vector_field_cache
    vector_field = property( _vector_field )

    def _vector_sql( self, field, weight=None ):
        if weight is None:
            weight = self.default_weight
        f = self.model._meta.get_field( field )
        return "setweight( to_tsvector( '%s', coalesce(\"%s\",'') ), '%s' )" % (self.config, f.column, weight)

    def update_index( self, pk=None ):
        from django.db import connection
        clauses = []
        if self.fields is None:
            self.fields = self._find_text_fields()
        if isinstance( self.fields, (list,tuple) ):
            for field in self.fields:
                clauses.append( self._vector_sql(field) )
        else:
            for field, weight in self.fields.items():
                clauses.append( self._vector_sql(field,weight) )
        vector_sql = ' || '.join( clauses )
        where = ''
        if pk is not None:
            if isinstance( pk, (list,tuple) ):
                ids = ','.join( [str(v) for v in pk] )
                where = " WHERE \"%s\" IN (%s)" % (self.model._meta.pk.column, ids)
            else:
                where = " WHERE \"%s\" = %s" % (self.model._meta.pk.column, pk)
        sql = "UPDATE \"%s\" SET \"%s\" = %s%s;" % (self.model._meta.db_table, self.vector_field.column, vector_sql, where)
        cursor = connection.cursor()
        cursor.execute( sql )

    def search( self, query, rank_field=None, rank_normalization=32, use_web_query=False ):
        if use_web_query:
            to_tsquery_string = "to_tsquery('%s',web_query('%s'))"
        else:
            to_tsquery_string = "to_tsquery('%s','%s')"

        cond = unicode(query).translate({
            ord(u"'"): u"''",
            ord(u"%"): None,
            ord(u"("): None,
            ord(u")"): None,
            ord(u"|"): None,
        })
        ts_query = to_tsquery_string % (self.config, cond)

        where = "\"%s\" @@ %s" % (self.vector_field.column, ts_query)
        select = {}
        order = []
        if rank_field is not None:
            select[rank_field] = 'ts_rank( "%s", %s, %d )' % (self.vector_field.column, ts_query, rank_normalization)
            order = ['-%s' % rank_field]
        return self.all().extra( select=select, where=[where], order_by=order )

