import warnings

warnings.filterwarnings('ignore', category=FutureWarning)
from flask import abort, render_template, Flask, request
import logging
import db

APP = Flask(__name__)


@APP.route('/')
def index():
    stats = {}
    stats = db.execute('''
                       SELECT *
                       FROM (SELECT ROUND(SUM(area_hectares) / 100, 2) AS total_area
                             FROM Sitios)
                                JOIN (SELECT COUNT(*) id_no FROM Sitios
                       )
                                JOIN
                            (SELECT COUNT(*) AS site_number1
                            FROM ( SELECT sitio FROM Sitio_Pais
                            GROUP BY sitio
                            HAVING COUNT(DISTINCT pais) > 1
                            ))
                                JOIN
                            (select count(*) site_number2 from Periodos_Perigo where data_inicio is not NULL and data_fim is NULL)
                                JOIN
                            (SELECT COUNT(*) site_number3
                             FROM Sitios
                             WHERE categoria = 'C')
                                JOIN
                            (SELECT COUNT(*) site_number4
                             FROM Sitios
                             WHERE categoria = 'C/N')
                                JOIN
                            (SELECT COUNT(*) site_number5
                             FROM Sitios
                             WHERE categoria = 'N')
                       ''').fetchone()
    logging.info(stats)
    return render_template('index.html', stats=stats)


@APP.route('/sites/<int:id>/')
def get_site(id):
    site = db.execute(
        '''
        SELECT s.id_no,
               s.nome,
               s.descricao,
               c.countries,
               p.regiao,
               l.latitude,
               l.longitude,
               s.area_hectares,
               s.data_inscricao,
               j.justificacao,
               CASE
                   WHEN pp.data_inicio IS NOT NULL AND pp.data_fim IS NULL THEN 1
                   ELSE 0
                   END AS danger,
               cat.categoria,
               s.categoria as categoria_short
        FROM Sitios s
                 LEFT JOIN (
            SELECT
                sp.sitio,
                GROUP_CONCAT(p.nome, ', ') AS countries,
                MIN(sp.pais) AS first_country
            FROM Sitio_Pais sp
            JOIN Paises p ON sp.pais = p.iso_code
            GROUP BY sp.sitio
        ) c ON c.sitio = s.id_no
        JOIN Paises p ON c.first_country = p.iso_code
        JOIN Localizacoes l on s.id_no = l.sitio
        LEFT JOIN Justificacoes j on s.id_no = j.sitio
        LEFT JOIN Periodos_Perigo pp on s.id_no = pp.sitio
        JOIN Categorias cat on s.categoria = cat.categoria_short
        WHERE s.id_no = ?
        GROUP BY s.id_no, s.nome, s.descricao, c.countries, p.regiao, l.latitude,
                 l.longitude, s.area_hectares, s.data_inscricao, j.justificacao,
                 danger, cat.categoria, s.categoria
        ''', [id]).fetchone()

    if site is None:
        abort(404, 'Site id {} does not exist.'.format(id))

    return render_template('site.html', site=site)


@APP.route('/sites/<int:id>/criteria/')
def get_criteria(id):
    criteria = db.execute(
        '''
        SELECT s.id_no, s.nome, j.criterio, cr.descricao, j.justificacao
        FROM Sitios s
                 JOIN Justificacoes j ON s.id_no = j.sitio
                 JOIN Criterios cr ON j.criterio = cr.id_criterio
        WHERE s.id_no = ?
        GROUP BY s.id_no, j.criterio
        ORDER BY CASE
                     WHEN j.criterio = 'N10' THEN 999
                     ELSE 1
                     END,
                 j.criterio;
        ''', (id,)).fetchall()
    return render_template('site-criteria.html', criteria=criteria)


def normalize(v):
    return "" if v in (None, "", "None") else v.strip()


@APP.route('/sites/')
def index_sites():
    country = request.args.get('country', '')
    year = request.args.get('year', '')
    category = request.args.get('category', '')
    danger = request.args.get('danger', '')

    sites, summary = execute_filter_query(country, year, category, danger)

    return render_template('sites-list.html',
                           initial_country=country,
                           initial_year=year,
                           initial_category=category,
                           initial_danger=danger,
                           initial_sites=sites,
                           initial_summary=summary)


def execute_filter_query(country, year, category_short, danger_status):
    country = normalize(country)
    year = normalize(year)
    category_short = normalize(category_short)
    danger_status = normalize(danger_status)

    base_sql = """
               SELECT s.id_no, \
                      s.nome, \
                      s.descricao, \
                      s.data_inscricao, \
                      s.categoria, \
                      CASE \
                          WHEN pp.data_inicio IS NOT NULL AND pp.data_fim IS NULL THEN 1 \
                          ELSE 0 \
                          END AS danger
               FROM Sitios s
                        LEFT JOIN Periodos_Perigo pp
                                  ON s.id_no = pp.sitio
               WHERE 1 = 1 \
               """

    params = []

    base_sql += """
            AND s.id_no IN (
                SELECT sp.sitio 
                FROM Sitio_Pais sp 
                JOIN Paises p ON sp.pais = p.iso_code 
                WHERE p.nome LIKE ?
            )
        """
    params.append('%' + country + '%')

    if year and year.isdigit():
        base_sql += " AND s.data_inscricao = ?"
        params.append(int(year))

    if category_short:
        base_sql += " AND s.categoria = ?"
        params.append(category_short)

    if danger_status:
        base_sql += " AND danger = ?"
        params.append(int(danger_status))

    base_sql += """
        GROUP BY s.id_no, s.nome, s.descricao
        ORDER BY s.id_no
    """

    sites = db.execute(base_sql, params).fetchall()

    total_found = len(sites)
    summary = f"Showing {total_found} sites."

    if total_found == 0:
        summary = "No site founds matching the filters."

    return sites, summary


@APP.route('/sites/filter')
def filter_sites():
    country = request.args.get('country')
    year = request.args.get('year')
    category_short = request.args.get('category')
    danger_status = request.args.get('danger')

    sites, summary = execute_filter_query(country, year, category_short, danger_status)

    return render_template('sites_list_partial.html', sites=sites, summary=summary)


#
@APP.route('/authors/')
def authors():
    return render_template('authors.html')


@APP.route('/analysis/')
def analysis():
    return render_template('analysis.html')


def execute_analysis_query(query_type):
    context = {
        'results': [],
        'headers': [],
        'column_keys': []
    }
    sql = None

    if query_type == 'area_by_category':
        sql = """
              select ca.category_short, SUM(lo.area_hectares) as total_area
              from Category ca
                       join Location lo on ca.site_number = lo.site_number
              group by ca.category_short; \
              """
        context['headers'] = ['Category', 'Area (ha)']
        context['column_keys'] = ['category_short', 'total_area']

    elif query_type == 'sites_by_country':
        sql = """
              SELECT TRIM(value)               AS state_name,
                     CAST(COUNT(*) AS INTEGER) AS total_sites
              FROM Place pl
                       JOIN json_each('["' || replace(pl.states_name_en, ',', '","') ||
                                      '"]')
              GROUP BY state_name
              ORDER BY total_sites DESC;
              """
        context['headers'] = ['Country', 'Site Count']
        context['column_keys'] = ['state_name', 'total_sites']

    elif query_type == 'dangerous_sites_by_region':
        sql = """
              select count(*) as total_count, pl.region_en
              from Place pl
                       join State_of_Danger st on pl.site_number = st.site_number
              where st.danger = 1
              group by pl.region_en
              order by total_count desc;
              """
        context['headers'] = ['Region', 'Endangered Sites']
        context['column_keys'] = ['region_en', 'total_count']

    elif query_type == 'sites_by_inscription_year':
        sql = """
              select count(*) as total_count, ad.date_inscribed
              from Associated_Dates ad
              group by ad.date_inscribed
              order by ad.date_inscribed asc;
              """
        context['headers'] = ['Inscription Year', 'Site Count']
        context['column_keys'] = ['date_inscribed', 'total_count']

    elif query_type == 'top_5_country_with_most_dangerous_sites':
        sql = """
              select pl.states_name_en, count(*) as total_count
              from State_of_Danger sd
                       natural join Place pl
              where sd.danger == 1
              group by pl.states_name_en
              order by total_count desc
                  LIMIT 5;
              """

        context['headers'] = ['Country', 'Site Count']
        context['column_keys'] = ['states_name_en', 'total_count']

    elif query_type == 'average_criteria_per_site_per_category':
        sql = """
                SELECT categoria, avg(num) AS avg_criterio
                FROM
                    (SELECT sitio, categoria, COUNT(*) AS num
                    FROM Justificacoes, Sitios
                    WHERE Justificacoes.sitio=Sitios.id_no
                    GROUP by sitio)
                GROUP BY categoria
                ORDER BY avg_criterio DESC;
              """

        context['headers'] = ['Category', 'Average Criteria']
        context['column_keys'] = ['categoria', 'avg_criterio']

    elif query_type == 'sites_with_park_in_name':
        sql = """
                SELECT id_no, nome
                FROM Sitios
                WHERE nome LIKE '%Park%';
              """
        context['headers'] = ['Site Id', 'Site Name']
        context['column_keys'] = ['id_no', 'nome']

    elif query_type == 'number_sites_located_multiple_countries_per_hemisphere':
        sql = """
                SELECT CASE
                WHEN latitude>=0 THEN 'Northern'
                ELSE 'Southern' END AS hemisphere, count(sitio) AS num_sitio
                FROM Localizacoes
                WHERE sitio IN
                    (SELECT sitio
                    FROM
                        (SELECT sitio, count(*) AS num
                        FROM Sitio_Pais
                        GROUP BY sitio
                        HAVING num>1))
                GROUP BY hemisphere
                ORDER BY num_sitio DESC;
              """
        context['headers'] = ['Hemisphere', 'Number of Sites']
        context['column_keys'] = ['hemisphere', 'num_sitio']

    elif query_type == 'avg_latitude_and_longitude_by_region':
        sql = """
                SELECT regiao, AVG(latitude) AS avg_latitude, AVG(longitude) AS avg_longitude
                FROM Localizacoes,
                    (SELECT DISTINCT sitio, regiao
                    FROM Sitio_Pais, Paises
                    WHERE Sitio_Pais.pais=Paises.iso_code) r
                WHERE Localizacoes.sitio=r.sitio
                GROUP BY regiao
                ORDER BY regiao ASC;
              """
        context['headers'] = ['Region', 'Average Latitude', 'Average Longitude']
        context['column_keys'] = ['regiao', 'avg_latitude', 'avg_longitude']

    elif query_type == 'top_criteria_for_unique_justification':
        sql = """
                SELECT criterio, COUNT(*) AS num_sites
                FROM Justificacoes
                WHERE justificacao LIKE '%unique%'
                GROUP BY criterio
                ORDER BY num_sites DESC LIMIT 5;
              """
        context['headers'] = ['Criteria', 'Site Count']
        context['column_keys'] = ['criterio', 'num_sites']

    if sql:
        context['results'] = db.execute(sql).fetchall()
    return context


@APP.route('/analysis/run')
def run_analysis_query():
    query_type = request.args.get('query_type')
    if not query_type:
        return ""
    try:
        context = execute_analysis_query(query_type)
        return render_template('analysis_partial.html', **context)

    except Exception as e:
        logging.error(f"Error executing analysis query: {e}")


if __name__ == '__main__':
    APP.run(debug=True)

