DROP TABLE IF EXISTS public.test_table;

CREATE TABLE IF NOT EXISTS public.test_table (
  id serial NOT NULL PRIMARY KEY,
  test_value text NOT NULL
);



INSERT INTO public.test_table (id, test_value) VALUES (0, 'test value 1');
INSERT INTO public.test_table (id, test_value) VALUES (1, 'test value 2');
INSERT INTO public.test_table (id, test_value) VALUES (2, 'test value 3');
INSERT INTO public.test_table (id, test_value) VALUES (3, 'test value 4');
INSERT INTO public.test_table (id, test_value) VALUES (4, 'test value 5');
INSERT INTO public.test_table (id, test_value) VALUES (5, 'test value 6');
INSERT INTO public.test_table (id, test_value) VALUES (6, 'test value 7');
INSERT INTO public.test_table (id, test_value) VALUES (7, 'test value 8');
INSERT INTO public.test_table (id, test_value) VALUES (8, 'test value 9');
INSERT INTO public.test_table (id, test_value) VALUES (9, 'test value 10');

