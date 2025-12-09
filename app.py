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
        SELECT id_no,
               name_en,
               short_description_en,
               place.states_name_en as countries,
               region_en,
               latitude,
               longitude,
               area_hectares,
               date_inscribed,
               justification_en,
               danger,
               category,
               cd.category_short
        FROM World_Heritage_Site
                 JOIN Location ON World_Heritage_Site.id_no = Location.site_number
                 JOIN place ON Location.site_number = place.site_number
                 JOIN associated_dates
                      ON World_Heritage_Site.id_no = associated_dates.site_number
                 JOIN state_of_danger
                      ON World_Heritage_Site.id_no = state_of_danger.site_number
                 JOIN Category c ON World_Heritage_Site.id_no = c.site_number
                 JOIN Category_Description cd ON c.category_short = cd.category_short
                 JOIN site_criteria
                      ON World_Heritage_Site.id_no = site_criteria.site_number
                 JOIN Criterion_Descriptions cr
                      ON site_criteria.criterion_code = cr.criterion_code
        WHERE id_no = ?
        GROUP BY id_no, name_en, short_description_en
        ''', [id]).fetchone()

    if site is None:
        abort(404, 'Site id {} does not exist.'.format(id))

    return render_template('site.html', site=site)


@APP.route('/sites/<int:id>/criteria/')
def get_criteria(id):
    criteria = db.execute(
        '''
        SELECT id_no, name_en, sc.criterion_code, cd.cd_description, wh.justification_en
        FROM World_Heritage_Site wh
                 JOIN Category c ON wh.id_no = c.site_number
                 JOIN site_criteria sc ON c.site_number = sc.site_number
                 JOIN criterion_descriptions cd ON sc.criterion_code = cd.criterion_code
        WHERE id_no = ?
        GROUP BY wh.id_no, sc.criterion_code
        ORDER BY CASE
                     WHEN sc.criterion_code = 'N10' THEN 999
                     ELSE 1
                     END,
                 sc.criterion_code;
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
               SELECT T1.id_no, T1.name_en, T1.short_description_en
               FROM World_Heritage_Site AS T1

                        -- Juntar todas as tabelas necessárias para os filtros potenciais
                        JOIN Location AS T2 ON T1.id_no = T2.site_number
                        JOIN Place AS T3 ON T1.id_no = T3.site_number
                        JOIN associated_dates AS T4 ON T1.id_no = T4.site_number
                        JOIN state_of_danger AS T5 ON T1.id_no = T5.site_number
                        JOIN Category AS T6 ON T1.id_no = T6.site_number

               WHERE 1 = 1
               """
    params = []

    if country:
        base_sql += " AND T3.states_name_en LIKE ?"
        params.append('%' + country + '%')

    if year and year.isdigit():
        base_sql += " AND T4.date_inscribed = ?"
        params.append(int(year))

    if category_short:
        base_sql += " AND T6.category_short = ?"
        params.append(category_short)

    if danger_status:
        base_sql += " AND T5.danger = ?"
        params.append(int(danger_status))

    base_sql += """
        GROUP BY T1.id_no, T1.name_en, T1.short_description_en
        ORDER BY T1.id_no
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
              SELECT c.category_short,
                     AVG(criteria_count) AS avg_criteria_per_site
              FROM (SELECT sc.site_number,
                           COUNT(*) AS criteria_count
                    FROM Site_Criteria sc
                    GROUP BY sc.site_number)
                       natural join Category c
              GROUP BY c.category_short
              ORDER BY avg_criteria_per_site DESC;
              """

        context['headers'] = ['Category', 'Average Criteria']
        context['column_keys'] = ['category_short', 'avg_criteria_per_site']

    elif query_type == 'sites_with_park_in_name':
        sql = """
              select ws.id_no, ws.name_en
              from World_Heritage_Site ws
              where ws.name_en like '%Park%';
              """
        context['headers'] = ['Site Id', 'Site Name']
        context['column_keys'] = ['id_no', 'name_en']

    elif query_type == 'number_sites_located_multiple_countries_per_hemisphere':
        sql = """
              SELECT CASE
                         WHEN loc.latitude >= 0 THEN 'Northern'
                         ELSE 'Southern' END AS hemisphere,
                     COUNT(ws.id_no)         AS site_count
              FROM World_Heritage_Site ws
                       JOIN Location loc on ws.id_no = loc.site_number
              WHERE loc.transboundary = 1
              GROUP BY hemisphere
              ORDER BY site_count DESC;
              """
        context['headers'] = ['Hemisphere', 'Number of Sites']
        context['column_keys'] = ['hemisphere', 'site_count']

    elif query_type == 'avg_latitude_and_longitude_by_region':
        sql = """
              SELECT TRIM(j.value)     AS region,
                     AVG(lo.latitude)  AS avg_latitude,
                     AVG(lo.longitude) AS avg_longitude
              FROM Location lo
                       NATURAL JOIN Place pl
                       JOIN json_each('["' || replace(pl.region_en, ',', '","') || '"]') AS j
              GROUP BY region
              ORDER BY region;
              """
        context['headers'] = ['Region', 'Average Latitude', 'Average Longitude']
        context['column_keys'] = ['region', 'avg_latitude', 'avg_longitude']

    elif query_type == 'top_criteria_for_unique_justification':
        sql = """
                SELECT criterio, COUNT(*) AS num_sites
                FROM Justificacoes
                WHERE justificacao LIKE "%unique%"
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

