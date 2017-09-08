package com.jn.service;

import com.jn.entity.Book;
import com.jn.entity.Car;
import com.jn.entity.Person;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:/repository-test-nested-object.xml")
public class ElasticTest {

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Before
    public void before() {
        elasticsearchTemplate.deleteIndex(Book.class);
        elasticsearchTemplate.createIndex(Book.class);
        elasticsearchTemplate.putMapping(Book.class);
        elasticsearchTemplate.refresh(Book.class);
        elasticsearchTemplate.deleteIndex(Person.class);
        elasticsearchTemplate.createIndex(Person.class);
        elasticsearchTemplate.putMapping(Person.class);
        elasticsearchTemplate.refresh(Person.class);
    }

    @Test
    public void shouldIndexInitialLevelNestedObject() {

        final List<Car> cars = new ArrayList<>();

        final Car saturn = new Car();
        saturn.setName("Saturn");
        saturn.setModel("SL");

        final Car subaru = new Car();
        subaru.setName("Subaru");
        subaru.setModel("Imprezza");

        final Car ford = new Car();
        ford.setName("Ford");
        ford.setModel("Focus");

        cars.add(saturn);
        cars.add(subaru);
        cars.add(ford);

        final Person foo = new Person();
        foo.setName("Foo");
        foo.setId("1");
        foo.setCar(cars);

        final Car car = new Car();
        car.setName("Saturn");
        car.setModel("Imprezza");

        final Person bar = new Person();
        bar.setId("2");
        bar.setName("Bar");
        bar.setCar(Arrays.asList(car));

        final List<IndexQuery> indexQueries = new ArrayList<>();
        final IndexQuery indexQuery1 = new IndexQuery();
        indexQuery1.setId(foo.getId());
        indexQuery1.setObject(foo);

        final IndexQuery indexQuery2 = new IndexQuery();
        indexQuery2.setId(bar.getId());
        indexQuery2.setObject(bar);

        indexQueries.add(indexQuery1);
        indexQueries.add(indexQuery2);

        elasticsearchTemplate.putMapping(Person.class);
        elasticsearchTemplate.bulkIndex(indexQueries);
        elasticsearchTemplate.refresh(Person.class);

        final QueryBuilder builder = nestedQuery("car", boolQuery().must(termQuery("car.name", "saturn")).must(termQuery("car.model", "imprezza")));

        final SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(builder).build();
        final List<Person> persons = elasticsearchTemplate.queryForList(searchQuery, Person.class);

        assertThat(persons.size(), is(1));
    }


}
