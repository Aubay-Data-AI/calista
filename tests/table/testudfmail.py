from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_udf

# 📌 Import du module contenant l'UDF `validate_email`
from calista.label.snowflake import init_udf  # ✅ Import correct

# 📌 Import des paramètres de connexion à Snowflake
from tests.table.parameters import SNOWFLAKE_CONN_PARAMS  # ✅ Utilisation correcte des paramètres de connexion



def main():
    """ Initialise l'UDF validate_email et teste son enregistrement dans Snowflake """
    
    # ✅ 1. Création de la session Snowflake en utilisant `SNOWFLAKE_CONN_PARAMS`
    try:
        session = Session.builder.configs(SNOWFLAKE_CONN_PARAMS).create()
        print("✅ Connexion à Snowflake réussie !")
    except Exception as e:
        print(f"❌ Erreur de connexion à Snowflake : {e}")
        return

    # ✅ 2. Enregistrement de l'UDF validate_email
    try:
        init_udf(session)
        print("✅ UDF 'validate_email' enregistrée avec succès !")
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement de l'UDF : {e}")
        return

    # ✅ 3. Vérification que l'UDF est bien enregistrée
    try:
        result = session.sql("SHOW USER FUNCTIONS").collect()
        udf_names = [row['name'].upper() for row in result]
        if "VALIDATE_EMAIL" in udf_names:
            print("✅ L'UDF 'validate_email' est bien enregistrée !")
        else:
            print("❌ L'UDF 'validate_email' n'est pas enregistrée.")
    except Exception as e:
        print(f"❌ Erreur lors de la vérification de l'UDF : {e}")
        return

    # ✅ 4. Tester l'UDF avec quelques emails
    test_cases = [
        ("valid.email@example.com", True),
        ("invalid-email.com", False),
        ("UPPERCASE@DOMAIN.COM", True),
        ("user@[192.168.1.1]", True),
        ("too..many..dots@example.com", False),
        ("invalid@no_tld", False),
        ("test@domain.toolongtld", False),
        ("éçàôù@example.com", True),  # Email avec caractères Unicode
        ("<john.doe@example.com>", False),  # Mauvaise syntaxe
    ]
    
    print("\n🔍 Résultats des tests :")
    for email, expected in test_cases:
        try:
            query = f"SELECT validate_email('{email}')"
            result = session.sql(query).collect()
            is_valid = result[0][0]  # Récupère le résultat du test

            # Vérification du résultat
            status = "✅ Réussi" if is_valid == expected else "❌ Échec"
            print(f"{status} | {email} → Attendu: {expected}, Obtenu: {is_valid}")

        except Exception as e:
            print(f"❌ Erreur lors du test de {email} : {e}")

    # ✅ 5. Fermeture de la session Snowflake
    session.close()
    print("\n✅ Session Snowflake fermée.")


if __name__ == "__main__":
    main()
