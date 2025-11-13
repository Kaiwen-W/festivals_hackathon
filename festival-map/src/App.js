import React, { useEffect, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup, useMapEvents } from "react-leaflet";
import "react-datepicker/dist/react-datepicker.css";
import DatePicker from "react-datepicker";
import L from "leaflet";

//import logo from './logo.svg';
import './App.css';


const mapIcon = () => 
  L.divIcon({
    className: "eventPoint",
    html: `<div style="
      width: 8px; height: 8px;
      border-radius: 50%;
      border:2px solid white;
      background: blue"></div>`,
    iconSize: [12,12]
  });

function PopupComponent({event, place}) {
  console.log("Popup!");
  console.log(place);
  console.log(event);
  let events = "events";
  if (event.length === 1) events="event";
  return (
    <div className="popupUnit">
      <strong>{place.name} </strong><p>{event.length} {events}</p>
      <div className="scrollUnit">
      {event.map((ev) => {
        var myDate = new Date(ev.start_ts);
        var dated = myDate.toLocaleString();
        return(
          <div><p>{ev.name} {dated}</p></div>
        )
      })}
      </div>
    </div>
  )
}

function EventsComponent({sdate, edate}) {

  const [places, setPlaces] = useState([]);
  const [events, setEvents] = useState([]);
  const [centre, setCentre] = useState([55.95, -3.18]);
  //const [zoom, setZoom] = useState(12);
  useEffect(() => { //places data
    fetch('thistle_data.json',{
      headers : { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
       }
    })
    .then((res) => res.json())
    .then((data) => {
      console.log("Fetched data");
      //console.log(data);
      
      const info = data.places.map((place) => ({
        address: place.address,
        name: place.name,
        id: place.place_id,
        lat: place.loc.latitude,
        lon: place.loc.longitude,
        tags: place.tags,
        town: place.town
      }));
      //console.log(info);

      let dict = {};
      info.forEach((el, index) => dict[el.id] = el);
      console.log(dict);
      setPlaces(dict);
    })
  },[]);

  /*useEffect(() => {
    fetch('thistle_data.json',{
      headers : { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
       }
    })
    .then((res) => res.json())
    .then((data) => {
      console.log("Fetched data");
      //console.log(data);

      const info = data.events;
      //console.log(info);
      setEvents(info);
    })
  },[]);*/


  function filterr(event, place) {
    const filtered = {
      event_id: event.event_id,
      name: event.name,
      status: event.status,
      start_ts: place.start_ts,
      end_ts: place.end_ts,
      performances: place.performances,
      place: place.place
    }

    return filtered
  }

  function validEvent(event) {
    
    var validated = []
    //console.log(event);
    //console.log(typeof(event));
    event.forEach((ev, index) => {
      //console.log(ev);
      var start = new Date(ev.start_ts);
      //var end = new Date(ev.end_ts);

      //console.log(start, end);
      //console.log(sdate, edate);
      if (start >= sdate && start <= edate) {
        validated.push(ev);
      } //therefore valid as after start date and before end date
    });
    return validated;
  }

  useEffect(() => {
    fetch('thistle_data.json',{
      headers : { 
        'Content-Type': 'application/json',
        'Accept': 'application/json'
       }
    })
    .then((res) => res.json())
    .then((data) => {
      console.log("Fetched data");
      //console.log(data);

      const info = data.events;
      //console.log(info);
      var dict = {};
      info.forEach((el, ind) => {

        el.schedules.forEach((place, index) => {
          if (dict[place.place_id]) {
            dict[place.place_id].push(filterr(el, place));
          }
          else {
            dict[place.place_id] = [filterr(el, place)];
          }
        })

      })
      console.log("dict below");
      console.log(dict);
      setEvents(dict);
    })
  },[])

  useMapEvents({
    zoomend: (e) => {
      //setZoom(e.target.getZoom());
      setCentre(e.target.getCenter());
    },
    moveend: (e) => {
      setCentre(e.target.getCenter());
    },
  });


  return (<>
    {
    Object.keys(events).map(function(index, number) { //index is place id
      let event = events[index];
      //console.log("ei", event, index);
      const latd = 0.010;
      const lond = 0.030;
      //console.log("toilet");
      if (places[index] !== undefined) {
        let temp = places[index]
        //console.log("first test");
        //console.log(centre.lat, temp.lat);
        //console.log(event);
        if ((Math.abs(temp.lat-centre.lat) <= latd && Math.abs(temp.lon-centre.lng) <= lond) || true) {
          //console.log("ohio toilet");
          //console.log(event.descriptions);
          let validEvents = validEvent(event);

          //here should work on filtering etc. then return length of new set of events, and submit those to the
          //popup for rendering
          if (validEvents.length > 0) {
            return (
              <Marker
                key = {index}
                position={[temp.lat, temp.lon]}
                icon={mapIcon()}
                total={validEvents.length}
              >
                <Popup>
                  <PopupComponent event={validEvents} place={temp}/>
                </Popup>
              </Marker>
            )
          }
          else return null;
        }
        else return null;
      }
      return null;
    })
  }
  </>
  )
}

function DateComponent({sdate, setsDate,edate, seteDate}) {


  return (
    <div className="menu" style={{padding: "20px"}}>

      <DatePicker enableTabLoop={false} style={{zIndex: 10}} selected={sdate} onChange={(sdate) => setsDate(sdate)} />
      <DatePicker enableTabLoop={false} style={{zIndex: 10}} selected={edate} onChange={(edate) => seteDate(edate)} />

    </div>
  );
}

export default function App() {
  const [sdate, setsDate] = useState(new Date());
  const [edate, seteDate] = useState(new Date());


  return (
    <>
    
    <DateComponent
     sdate={sdate} 
     setsDate={setsDate}
     edate={edate}
     seteDate={seteDate}
     style={{minHeight: "40px", margin: "2px"}}
      
    />

    <MapContainer
      center={[55.89, -3.72]}
      zoom={10}
      style={{width: "100%", height: "100vh", zIndex: "0"}}
    >
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
      />

      <EventsComponent 
        sdate={sdate}
        edate={edate}
      />

    </MapContainer>
    </>
  );
}
