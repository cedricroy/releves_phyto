import pandas as pd
import psycopg2
from qgis.core import QgsProcessingAlgorithm, QgsProcessingParameterFileDestination, QgsProcessingParameterString, QgsMessageLog

class TransformPostgreSQLToExcel(QgsProcessingAlgorithm):
    OUTPUT = 'OUTPUT'  # Path to output Excel file
    RELEVES = 'RELEVES'  # New parameter for filtering by number of relevé

    def initAlgorithm(self, config=None):
        self.addParameter(QgsProcessingParameterFileDestination(
            self.OUTPUT,
            'Output Excel file',
            'Excel files (*.xlsx)',
            defaultValue='C:/Users/Cedric/Desktop/phyto.xlsx'
        ))
        
        self.addParameter(QgsProcessingParameterString(
            self.RELEVES,
            'Filter by numéros de relevé (comma-separated)',
            defaultValue='20240618CB01,T6-C5/1'
        ))

    def processAlgorithm(self, parameters, context, feedback):
        output_file = self.parameterAsFileOutput(parameters, self.OUTPUT, context)
        releves_filter = self.parameterAsString(parameters, self.RELEVES, context)

        # Database connection parameters
        conn_params = {
            'dbname': 'xx',
            'user': 'xx',
            'password': 'xx',
            'host': 'xx.fr',
            'port': '5432'
        }
        
        # Create the filter condition
        filter_condition = self._build_filter_condition(releves_filter)
        
        # Connect to the PostgreSQL database
        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()

            # Execute the query
            query = f"SELECT * FROM geonature.v_releves_phytosocioceno WHERE {filter_condition}"
            cursor.execute(query)

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

        # Handle lb_nom, indice_abondance_dominance, and strate_vegetation
        if 'lb_nom' in df.columns and 'indice_abondance_dominance' in df.columns:
            taxon_data = df[['numero_releve', 'lb_nom', 'indice_abondance_dominance', 'strate_vegetation', 'type_releve']].dropna(subset=['lb_nom']).drop_duplicates()

            for releve in unique_releves:
                releve_data = taxon_data[taxon_data['numero_releve'] == releve]
                
                # Préparer les listes de taxon_rows pour chaque relevé
                taxon_rows = []
                
                for _, row in releve_data.iterrows():
                    taxon = row['lb_nom']
                    strate = row['strate_vegetation']
                    abundance = row['indice_abondance_dominance']
                    type_releve = row['type_releve']
                    
                    # Format the taxon row correctly based on whether strate_vegetation is present
                    if pd.notna(strate):
                        taxon_row = f";{strate};{taxon}"
                    else:
                        taxon_row = f";{taxon}"
                    
                    # Adjust the abundance based on type_releve
                    if type_releve == 'Relevé phytosociologique':
                        # Transform values
                        if pd.notna(abundance):
                            if abundance == '+ : Individus peu abondants, recouvrement inférieur à 5% de la surface':
                                abundance = '0.5'
                            elif abundance == 'i : Individu unique':
                                abundance = '0.1'
                            elif abundance == 'r : Individus très rares, recouvrant moins de 1% de la surface':
                                abundance = '0.2'
                            elif abundance == '':
                                abundance = '0'
                            else:
                                abundance = str(abundance)[0] if pd.notna(abundance) else '1'  # Default to first character
                        else:
                            abundance = '1'
                        taxon_row += f";{abundance}"
                    elif type_releve == 'Relevé phytocénotique':
                        taxon_row += ";1"
                    else:
                        taxon_row += f";{abundance if pd.notna(abundance) else '1'}"
                    
                    taxon_rows.append(taxon_row)
                
                # Convertir les taxon_rows en DataFrame pour séparer les colonnes
                if taxon_rows:
                    df_taxon = pd.DataFrame(taxon_rows, columns=['Combined'])
                    df_taxon[['Strate', 'Taxon', 'Abundance']] = df_taxon['Combined'].str.split(';', expand=True)
                    
                    # Créer une colonne temporaire pour le tri par strate
                    strata_order = {
                        'Strate arborée': 1,
                        'Strate arbustive': 2,
                        'Strate herbacée': 3
                    }
                    df_taxon['strate_order'] = df_taxon['Strate'].map(strata_order).fillna(4)  # Fillna with 4 for empty or unrecognized strata
                    
                    # Trier les lignes par 'strate_order', puis par 'Taxon'
                    df_taxon_sorted = df_taxon.sort_values(by=['strate_order', 'Taxon'])
                    
                    # Retirer la colonne temporaire
                    df_taxon_sorted = df_taxon_sorted.drop(columns=['strate_order'])
                    
                    # Créer une ligne par Strate et Taxon
                    for _, row in df_taxon_sorted.iterrows():
                        taxon_row = [row['Strate'] if pd.notna(row['Strate']) else '']
                        taxon_row.append(row['Taxon'])
                        
                        for r in unique_releves:
                            if r == releve:
                                if type_releve == 'Relevé phytosociologique':
                                    taxon_row.append(row['Abundance'])
                                elif type_releve == 'Relevé phytocénotique':
                                    taxon_row.append('1')
                                else:
                                    taxon_row.append(row['Abundance'] if pd.notna(row['Abundance']) else '1')
                            else:
                                taxon_row.append('')
                        
                        result.append(taxon_row)

        # Write the results to an Excel file
        df_result = pd.DataFrame(result)
        df_result.to_excel(output_file, index=False, header=False)

        feedback.pushInfo(f"Transformation completed. Results saved to '{output_file}'.")

        return {}

    def _build_filter_condition(self, releves_filter):
        releves_list = releves_filter.split(',')
        releves_list = [r.strip() for r in releves_list if r.strip()]  # Clean up any extra spaces
        if releves_list:
            # Use LIKE operator for partial matching
            releves_condition = " OR ".join([f"numero_releve LIKE '%{r}%'" for r in releves_list])
            return releves_condition
        return ''

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
