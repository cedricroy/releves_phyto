import pandas as pd
import psycopg2
from qgis.core import QgsProcessingAlgorithm, QgsProcessingParameterFileDestination, QgsProcessingParameterString, QgsMessageLog

class TransformPostgreSQLToExcel(QgsProcessingAlgorithm):
    OUTPUT = 'OUTPUT'  # Path to output Excel file
    RELEVES = 'RELEVES'  # Parameter for filtering by number of relevé
    OBSERVATEUR = 'OBSERVATEUR'  # Parameter for filtering by observer
    DATE = 'DATE'  # Parameter for filtering by date

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFileDestination(
            self.OUTPUT,
            'Fichier Excel de sortie',
            'Excel files (*.xlsx)',
            defaultValue=''
        ))

        self.addParameter(QgsProcessingParameterString(
            self.RELEVES,
            'Filtrer par numéros de relevé (séparés par une virgule)',
            defaultValue='',
            optional=True  # Marking this parameter as optional
        ))

        self.addParameter(QgsProcessingParameterString(
            self.OBSERVATEUR,
            'Filtrer par observateurs (séparés par une virgule)',
            defaultValue='',
             optional=True  # Marking this parameter as optional
       ))

        self.addParameter(QgsProcessingParameterString(
            self.DATE,
            'Filtrer par date (séparées par une virgule, format YYYY-MM-DD)',
            defaultValue='',
            optional=True  # Marking this parameter as optional
        ))

    def processAlgorithm(self, parameters, context, feedback):
        output_file = self.parameterAsFileOutput(parameters, self.OUTPUT, context)
        releves_filter = self.parameterAsString(parameters, self.RELEVES, context)
        observateur_filter = self.parameterAsString(parameters, self.OBSERVATEUR, context)
        date_filter = self.parameterAsString(parameters, self.DATE, context)

      # Database connection parameters
        conn_params = {
            'dbname': 'x',
            'user': 'x',
            'password': 'x',
            'host': 'x.fr',
            'port': '5432'
        }   
        
        # Create the filter condition using the provided filters
        filter_condition, params = self._build_filter_condition(releves_filter, observateur_filter, date_filter)

        # Connect to the PostgreSQL database
        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Execute the query with the dynamically generated filter condition
            query = f"SELECT * FROM geonature.v_releves_phytosocioceno WHERE {filter_condition}"
            cursor.execute(query, params)

            # Fetch the data
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=columns)

            # Close the connection
            cursor.close()
            conn.close()

        except Exception as e:
            error_message = f'Failed to connect or query database: {str(e)}'
            QgsMessageLog.logMessage(error_message, 'TransformPostgreSQLToExcel', QgsMessageLog.CRITICAL)
            feedback.reportError(error_message)
            return {}

        # Ensure 'unique_id_sinp' is treated as the unique identifier
        if 'unique_id_sinp' not in df.columns:
            error_message = 'Column "unique_id_sinp" not found in the data'
            QgsMessageLog.logMessage(error_message, 'TransformPostgreSQLToExcel', QgsMessageLog.CRITICAL)
            feedback.reportError(error_message)
            return {}

        # Create a DataFrame to hold the results
        result = []

        # Define a mapping from column names to desired rows
        category_mapping = {
            'numero_releve': 'numero_releve',
            'observateurs': 'observateurs',
            'date_min': 'date_min',
            'date_max': 'date_max',
            'altitude_min': 'altitude_min',
            'altitude_max': 'altitude_max',
            'pente': 'pente',
            'exposition': 'exposition',
            'roche_mere': 'roche_mere',
            'topographie': 'topographie',
            'type_humus': 'type_humus',
            'surface': 'surface',
            'recouvrement_litiere': 'recouvrement_litiere',
            'recouvrement_solnu': 'recouvrement_solnu',
            'strate_arboree_hauteur': 'strate_arboree_hauteur',
            'strate_arboree_recouvrement': 'strate_arboree_recouvrement',
            'strate_arbustive_hauteur': 'strate_arbustive_hauteur',
            'strate_arbustive_recouvrement': 'strate_arbustive_recouvrement',
            'strate_herbacee_hauteurmoyenne': 'strate_herbacee_hauteurmoyenne',
            'strate_herbacee_recouvrement': 'strate_herbacee_recouvrement',
            'strate_muscinale_recouvrementtotal': 'strate_muscinale_recouvrementtotal',
            'strate_muscinale_recouvrementsphaigne': 'strate_muscinale_recouvrementsphaigne',
            'type_releve': 'type_releve'
        }

        # Process general information first
        for category, csv_header in category_mapping.items():
            if category in df.columns:
                values_by_releve = df.groupby('numero_releve')[category].apply(lambda x: ';'.join(map(str, x.dropna().unique())))
                row = [''] + [csv_header] + [values_by_releve.get(releve, '') for releve in df['numero_releve'].dropna().unique()]
                result.append(row)

        # Add an empty row as a separator
        result.append([''])

        # Get unique releve numbers
        unique_releves = df['numero_releve'].dropna().unique()

        # Gestion des taxons uniques par strate
        if 'lb_nom' in df.columns and 'indice_abondance_dominance' in df.columns:
            taxon_data = df[['numero_releve', 'lb_nom', 'indice_abondance_dominance', 'strate_vegetation', 'type_releve']].dropna(subset=['lb_nom']).drop_duplicates()

        taxon_data['strate_vegetation'] = taxon_data['strate_vegetation'].fillna('Non stratifié')
        taxon_grouped = taxon_data.groupby('strate_vegetation')['lb_nom'].unique()

        # Définir l'ordre des strates
        strate_order = {
            'Strate arborée': 1,
            'Strate arbustive': 2,
            'Strate herbacée': 3,
            'Non stratifié': 4  # Pour les valeurs nulles ou non stratifiées
        }

        # Trier les strates selon l'ordre défini, puis trier les taxons dans chaque strate par ordre alphabétique
        sorted_strate_taxon_pairs = sorted(
            taxon_grouped.items(),
            key=lambda x: (strate_order.get(x[0], 5), sorted(x[1]))
        )

        # Processus des taxons triés par strate pour chaque relevé
        for strate, taxons in sorted_strate_taxon_pairs:
            # Taxons triés par ordre alphabétique au sein de chaque strate
            sorted_taxons = sorted(taxons)

            for taxon in sorted_taxons:
                taxon_row = [strate, taxon]

                for releve in unique_releves:
                    releve_data = taxon_data[
                        (taxon_data['numero_releve'] == releve) &
                        (taxon_data['lb_nom'] == taxon) &
                        (taxon_data['strate_vegetation'] == strate)
                    ]

                    if not releve_data.empty:
                        abundance = releve_data['indice_abondance_dominance'].values[0]
                        type_releve = releve_data['type_releve'].values[0]

                        if type_releve == 'Relevé phytosociologique':
                            if abundance == '+ : Individus peu abondants, recouvrement inférieur à 5% de la surface':
                                abundance = '0.5'
                            elif abundance == 'i : Individu unique':
                                abundance = '0.1'
                            elif abundance == 'r : Individus très rares, recouvrant moins de 1% de la surface':
                                abundance = '0.2'
                            elif pd.isna(abundance):
                                abundance = '0'
                            else:
                                abundance = str(abundance)[0] if pd.notna(abundance) else '1'
                        elif type_releve == 'Relevé phytocénotique':
                            abundance = '1'
                    else:
                        abundance = ''

                    taxon_row.append(abundance)

                result.append(taxon_row)

        # Écrire les résultats dans un fichier Excel
        df_result = pd.DataFrame(result)
        df_result.to_excel(output_file, index=False, header=False)

        feedback.pushInfo(f"Transformation complétée. Résultats sauvegardés dans '{output_file}'.")

        return {}

    def _build_filter_condition(self, releves_filter, observateur_filter, date_filter):
        conditions = []
        params = []

        # Filter by releve numbers if provided
        if releves_filter:
            releves_list = [r.strip() for r in releves_filter.split(',') if r.strip()]
            if releves_list:
                conditions.append(" OR ".join(["numero_releve LIKE %s" for _ in releves_list]))
                params.extend([f'%{r}%' for r in releves_list])

        # Filter by observer if provided
        if observateur_filter:
            observateurs_list = [o.strip() for o in observateur_filter.split(',') if o.strip()]
            if observateurs_list:
                conditions.append(" OR ".join(["observateurs LIKE %s" for _ in observateurs_list]))
                params.extend([f'%{o}%' for o in observateurs_list])

        # Filter by date if provided
        if date_filter:
            dates_list = [d.strip() for d in date_filter.split(',') if d.strip()]
            if dates_list:
                conditions.append(" OR ".join(["date_min = %s" for _ in dates_list]))
                params.extend(dates_list)

        # If no filters are provided, return a condition that matches all rows
        if not conditions:
            return '1=1', params  # No filters, fetch all
        else:
            return ' AND '.join(f"({condition})" for condition in conditions), params

    def name(self):
        return 'extraction_relevephyto_geonature'

    def displayName(self):
        return 'Extraire les relevés phyto de GeoNature'

    def group(self):
        return 'Relevés phyto'

    def groupId(self):
        return 'releves_phyto'

    @staticmethod
    def createInstance():
        return TransformPostgreSQLToExcel()
