// LATO GODBEE
// Si attiva se AirHive è offline.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	firebase "firebase.google.com/go"
	"firebase.google.com/go/messaging"
	"google.golang.org/api/option"
)

// Il database viene aperto solo una volta.
var dbSource *sql.DB
var dbDest *sql.DB

// La scritta nella pagina
var messaggio string

// Lista aggiornata all'ultimo ciclo con AirHive up dei token di firebase
var listaToken []string

// Se è True ho già detto che AirHive è offline o che ho il controllo
var hoAvvisatoDiAirHive bool
var hoAvvisatoDelControllo bool

// Go non va in TimeOut mai se non glielo dici
var netClient = &http.Client{
	Timeout: time.Second * 30,
}

func initializeAppWithServiceAccount() *firebase.App {
	// [START initialize_app_service_account]
	opt := option.WithCredentialsFile("/home/giulio/go/src/transfer/serverstatus-35255-firebase-adminsdk-6xn73-c8b7cabf0b.json")
	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		log.Printf("Errore nell'avvio della app firebase: %v\n", err)
		return nil
	}
	// [END initialize_app_service_account]

	return app
}

func inviaMessaggio(app *firebase.App, registrationToken string, messaggioNotifica string) {
	// Obtain a messaging.Client from the App.
	ctx := context.Background()
	client, err := app.Messaging(ctx)
	if err != nil {
		log.Printf("Errore nel collegarsi al Messaging client: %v\n", err)
		return

	}
	// See documentation on defining a message payload.
	message := &messaging.Message{
		Notification: &messaging.Notification{
			Title: "GodBee",
			Body:  messaggioNotifica,
		},
		Android: &messaging.AndroidConfig{
			Priority: "high",
			Notification: &messaging.AndroidNotification{
				Color: "#FFBF00",
			},
		},
		Token: registrationToken,
	}

	// Send a message to the device corresponding to the provided
	// registration token.
	response, err := client.Send(ctx, message)
	if err != nil {
		log.Printf("Errore nell'inviare il messaggio: %v", err)
		return
	}
	// Response is a message ID string.
	log.Printf("Messaggio inviato con successo: %v, %v", messaggioNotifica, response)
}

func inviaNotifica(messaggioNotifica string) {
	app := initializeAppWithServiceAccount()
	if app == nil {
		log.Printf("Non posso inviare il messaggio.")
		return
	}
	for _, registrationToken := range listaToken {
		inviaMessaggio(app, registrationToken, messaggioNotifica)
	}
}

func transfer() error {

	var idSensore string
	var pm10 float32
	var temp float32
	var umi float32
	var prec float32
	var vento float32
	var no2 float32
	var o3 float32
	var tempo string
	var uniid string

	datiOrigine, err := dbSource.Query(fmt.Sprintf("SELECT * FROM sensori ORDER BY tempo DESC, id_sensore ASC LIMIT %d", 100))
	if err != nil {
		log.Printf("Errore nel prendere i dati dal Source: %v", err)
		return err
	}
	defer datiOrigine.Close()
	dbDestOp, err := dbDest.Begin()
	if err != nil {
		log.Printf("Errore nell'aprire il Dest: %v", err)
		return err
	}
	for datiOrigine.Next() {
		err = datiOrigine.Scan(&idSensore, &pm10, &temp, &umi, &prec, &vento, &no2, &o3, &tempo)
		if err != nil {
			log.Printf("%v", err)
		}
		tempo = strings.Replace(tempo, " ", "T", -1)
		uniid = strings.Join([]string{idSensore, tempo}, "")

		query := fmt.Sprintf(
			"INSERT INTO merano (id_sensore, pm10, temp, umi, prec, vento, no2, o3, tempo, uniid) VALUES ('%s', %f, %f, %f, %f, %f, %f, %f, '%s', '%s')",
			idSensore, pm10, temp, umi, prec, vento, no2, o3, tempo, uniid)
		_, err := dbDestOp.Exec(query)
		if err != nil {
			duplicato := strings.Contains(err.Error(), "Error 1062: Duplicate entry")
			if !duplicato && err != nil {
				log.Printf("Errore nell'inviare i dati al Dest: %v", err)
				return err
			}
		}
	}
	err = dbDestOp.Commit()
	if err != nil {
		log.Printf("Errore nel commit al Dest: %v", err)
		return err
	}
	return nil
}

func aggiornaFirebaseToken() []string {
	var listaCorrezione []string
	dbFire, err := sql.Open("mysql", "PASS")
	if err != nil {
		log.Printf("Errore nell'aprire la connessione col database firebase: %v", err)
		return nil
	}
	defer dbFire.Close()

	datiToken, err := dbFire.Query(fmt.Sprintf("SELECT token FROM firebase"))
	if err != nil {
		log.Printf("Errore nel leggere il database firebase: %v", err)
		return nil
	}
	var token string
	for datiToken.Next() {
		datiToken.Scan(&token)
		listaCorrezione = append(listaCorrezione, token)
	}
	return listaCorrezione
}

func transferContinuo() {
	//Trasferisce i dati, aggiorna lo stato e aggiorna i token
	var responseData []byte
	var err error
	for true {
		response, erroreConnessione := netClient.Get("https://house.zan-tech.com/status/")
		if erroreConnessione == nil {
			responseData, err = ioutil.ReadAll(response.Body)
			if err != nil {
				log.Printf("Errore nel leggere la risposta della get: %v", err)
			}
		} else {
			responseData = nil
		}
		if string(responseData) != "OK" || erroreConnessione != nil {
			err := transfer()
			if err != nil {
				messaggio = "ERRORE"
				if !hoAvvisatoDelControllo {
					inviaNotifica("ERRORE NEL PRENDERE IL CONTROLLO, AirHive offline.")
					hoAvvisatoDelControllo = true
				}
			} else {
				messaggio = "OK"
				if !hoAvvisatoDiAirHive {
					inviaNotifica("AirHive offline, prendo il controllo.")
					hoAvvisatoDiAirHive = true
					hoAvvisatoDelControllo = false
				}
			}
			//Aggiorna la cache su AirHive.it
			_, err = netClient.Get("https://www.airhive.it/data/updateDataCache.php?key=JDETLJimzBQJRbYloxS5Uio7PAvi6grDSOA4CLdY")
			if err != nil {
				inviaNotifica("Problema nell'aggiornare la cache.")
				log.Printf("Errore nel richiedere di aggiornare la cache: %v", err)
			}
			time.Sleep(14 * time.Minute)
		} else {
			messaggio = "ATTENDO"
			res := aggiornaFirebaseToken()
			if res != nil {
				listaToken = res
			}
			hoAvvisatoDiAirHive = false
			hoAvvisatoDelControllo = false
			time.Sleep(1 * time.Minute)
		}
	}
}

func status(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(messaggio))
}

func main() {
	// Apre connessione al db modificando la globale
	var err error
	messaggio = "START"
	listaToken = aggiornaFirebaseToken()
	hoAvvisatoDelControllo = false
	hoAvvisatoDiAirHive = false

	//TODO Correggere col database di LORA
	dbSource, err = sql.Open("mysql", "PASS2")
	if err != nil {
		log.Fatalf("Errore nell'aprire la connessione col database source: %v", err)
		return
	}
	defer dbSource.Close()

	dbSource.SetMaxIdleConns(20)
	dbSource.SetMaxOpenConns(0)
	dbSource.SetConnMaxLifetime(time.Minute * 20)

	err = dbSource.Ping()
	if err != nil {
		log.Fatalf("Errore nel mantenere la connessione col database source: %v", err)
		return
	}

	dbDest, err = sql.Open("mysql", "PASS3")
	if err != nil {
		log.Fatalf("Errore nell'aprire la connessione col database destinazione: %v", err)
		return
	}
	defer dbDest.Close()

	dbDest.SetMaxIdleConns(20)
	dbDest.SetMaxOpenConns(0)
	dbDest.SetConnMaxLifetime(time.Minute * 20)

	err = dbDest.Ping()
	if err != nil {
		log.Fatalf("Errore nel mantenere la connessione col database destinazione: %v", err)
		return
	}

	inviaNotifica("Avvio.")

	go transferContinuo()

	http.HandleFunc("/", status)
	err = http.ListenAndServe(":9090", nil)
	if err != nil {
		log.Fatalf("Errore nell'aprire la porta con tls: %v", err)
	}

}
