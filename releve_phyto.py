import pandas as pd
import psycopg2
from qgis.core import QgsProcessingAlgorithm, QgsProcessingParameterFileDestination, QgsProcessingParameterString, QgsMessageLog

class TransformPostgreSQLToExcel(QgsProcessingAlgorithm):
    OUTPUT = 'OUTPUT'  # Path to output Excel file
    RELEVES = 'RELEVES'  # Parameter for filtering by number of relevé

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFileDestination(
            self.OUTPUT,
            'Fichier Excel de sortie',
            'Excel files (*.xlsx)',
            defaultValue='C:/Users/Cedric/Desktop/phyto.xlsx'
        ))
        
        self.addParameter(QgsProcessingParameterString(
            self.RELEVES,
            'Filtrer par numéros de relevé (séparés par une virgule)',
            defaultValue='20240618CB01,T6-C5/1'
        ))

    def processAlgorithm(self, parameters, context, feedback):
        output_file = self.parameterAsFileOutput(parameters, self.OUTPUT, context)
        releves_filter = self.parameterAsString(parameters, self.RELEVES, context)

        # Database connection parameters
        conn_params = {
            'dbname': 'x',
            'user': 'x',
            'password': 'x',
            'host': 'x.fr',
            'port': '5432'
        }        
        # Create the filter condition
        filter_condition, params = self._build_filter_condition(releves_filter)
        
        # Connect to the PostgreSQL database
        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Execute the query
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

        taxon_data['strate_vegetation'] = taxon_data['strate_vegetation'].fillna('')
        taxon_grouped = taxon_data.groupby('strate_vegetation')['lb_nom'].unique()

        # Définir l'ordre des strates
        strate_order = {
            'Strate arborée': 1,
            'Strate arbustive': 2,
            'Strate herbacée': 3,
            'Non-Stratifié': 4  # Pour les valeurs nulles ou non stratifiées
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




    def _build_filter_condition(self, releves_filter):
        releves_list = releves_filter.split(',')
        releves_list = [r.strip() for r in releves_list if r.strip()]  # Clean up any extra spaces
        if releves_list:
            # Use placeholders and parameters for safe query construction
            placeholders = " OR ".join(["numero_releve LIKE %s" for _ in releves_list])
            return placeholders, tuple(f'%{r}%' for r in releves_list)
        return '', ()

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
