package org.example.ticketservice.models.services.impl;

import lombok.RequiredArgsConstructor;
import org.example.ticketservice.models.constants.TicketStatus;
import org.example.ticketservice.models.dto.requests.CreateTicketRequest;
import org.example.ticketservice.models.dto.requests.TicketEvent;
import org.example.ticketservice.models.dto.responses.TicketDetailResponse;
import org.example.ticketservice.models.dto.responses.TicketResponse;
import org.example.ticketservice.models.dto.responses.TripResponse;
import org.example.ticketservice.models.entities.Ticket;
import org.example.ticketservice.models.entities.TicketDetail;
import org.example.ticketservice.models.repositories.TicketDetailRepository;
import org.example.ticketservice.models.repositories.TicketRepository;
import org.example.ticketservice.models.services.TicketService;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TicketServiceImpl implements TicketService {
    private final TicketRepository ticketRepository;
    private final TicketDetailRepository ticketDetailRepository;
    private final TripGatewayService tripGatewayService;
    private final KafkaTemplate<String, TicketEvent> kafkaTemplate;

    @Override
    @Transactional
    public TicketResponse createTicket(CreateTicketRequest request) {
        List<TripResponse> tripResponses = request.items().stream().map(item -> tripGatewayService.getTripById(item.tripId())).toList();
        for (TripResponse tripResponse : tripResponses) {
            Integer seats = request.items().stream()
                    .filter(item -> item.tripId().equals(tripResponse.id()))
                    .findFirst()
                    .get()
                    .seat();
            if (seats <= 0 || seats > tripResponse.availableSeats()) {
                throw new IllegalArgumentException("Số ghế đặt không hợp lệ");
            }
        }

        double totalAmount = 0.0;
        for (TripResponse tripResponse : tripResponses) {
            Integer seats = request.items().stream().filter(i -> i.tripId().equals(tripResponse.id()))
                    .findFirst()
                    .get()
                    .seat();
            double lineTotal = tripResponse.ticketPrice() * seats;
            totalAmount += lineTotal;
        }

        Ticket ticket = Ticket.builder()
                .passengerName(request.passengerName())
                .passengerEmail(request.passengerEmail())
                .status(TicketStatus.PENDING)
                .totalAmount(totalAmount)
                .build();

        Ticket finalTicket = ticketRepository.save(ticket);

        List<TicketDetail> ticketDetails = tripResponses.stream().map(trip -> {
            Integer seats = request.items().stream().filter(i -> i.tripId().equals(trip.id()))
                    .findFirst().get().seat();
            return TicketDetail.builder()
                    .ticket(finalTicket)
                    .seats(seats)
                    .tripId(trip.id())
                    .ticketPrice(trip.ticketPrice())
                    .lineTotal(trip.ticketPrice() * seats)
                    .build();
        }).toList();

        ticketDetails = ticketDetailRepository.saveAll(ticketDetails);

        List<TicketDetailResponse> ticketDetailResponses = ticketDetails.stream().map(detail ->
                new TicketDetailResponse(
                        ticket.getId(),
                        detail.getTripId(),
                        detail.getSeats(),
                        detail.getTicketPrice(),
                        detail.getLineTotal()
                )).toList();

        TicketResponse ticketResponse = new TicketResponse(
                ticket.getId(),
                ticket.getPassengerName(),
                ticket.getPassengerEmail(),
                ticket.getTotalAmount(),
                ticket.getStatus(),
                ticketDetailResponses
        );

        kafkaTemplate.send("ticket-created",
                ticket.getId().toString(),
                new TicketEvent(ticket.getId(), ticket.getPassengerEmail())
        );
        return ticketResponse;
    }
}
