package io.github.jacekkardys.systemproof.environment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import io.github.jacekkardys.systemproof.control.SemanticControls;
import io.github.jacekkardys.systemproof.control.SemanticHold;
import io.github.jacekkardys.systemproof.control.SemanticHoldFailure;
import io.github.jacekkardys.systemproof.control.SemanticHoldRef;
import io.github.jacekkardys.systemproof.control.SemanticHoldState;
import io.github.jacekkardys.systemproof.control.SemanticInteractionSelector;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorBoundary;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuard;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardFailure;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardRef;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardSpec;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorGuardState;
import io.github.jacekkardys.systemproof.control.SemanticPredecessorViolation;
import io.github.jacekkardys.systemproof.environment.ProofSubjectRegistry.NativeFlowResolution;
import io.github.jacekkardys.systemproof.journal.SemanticPredecessorGuardEvent;
import io.github.jacekkardys.systemproof.observation.EvidenceSnapshot;
import io.github.jacekkardys.systemproof.observation.ForwardingDecision;
import io.github.jacekkardys.systemproof.observation.ForwardingPermit;
import io.github.jacekkardys.systemproof.observation.InteractionDecisionCoordinator;
import io.github.jacekkardys.systemproof.observation.InteractionRef;
import io.github.jacekkardys.systemproof.observation.RecordedInteraction;
import io.github.jacekkardys.systemproof.proof.CorrelationKey;
import io.github.jacekkardys.systemproof.proof.ProofSubjectRef;
import io.github.jacekkardys.systemproof.topology.ConnectionId;

/**
 * Environment-owned semantic-control registry, matcher, and linearization point.
 *
 * <p>A permit owns this monitor before entering the authoritative proof-operation boundary. Only
 * inside that boundary does it execute selectors, take detached subject-correlation snapshots, and
 * commit the transport decision. Subject mutation therefore completes before selection or waits
 * until after the decision; selector code never runs under the subject or proof monitor.
 * Terminal proof finalization is handed off until mandatory internal permit actions run and public
 * completion roots are submitted behind the immutable-result publication gate.
 */
final class SemanticControlCoordinator
    implements SemanticControls, InteractionDecisionCoordinator {

    private static final long FIRST_CONTROL_VALUE = 1L;
    private static final int MAXIMUM_CONTROLS = 256;
    private static final int MAXIMUM_PUBLIC_COMPLETIONS = MAXIMUM_CONTROLS * 3;
    private static final ForwardingPermit IMMEDIATE_FORWARD =
        new TerminalPermit(ForwardingDecision.FORWARD);
    private static final ForwardingPermit CLOSE_SESSION =
        new TerminalPermit(ForwardingDecision.CLOSE_SESSION);

    private final Object holdOwner = new Object();
    private final Object guardOwner = new Object();
    private final EnvironmentEventPublisher events;
    private final ProofSubjectRegistry proofSubjects;
    private final SemanticControlCapabilityRegistry controlCapabilities;
    private final TimeoutScheduler timeoutScheduler;
    private final ProofObservationListener proofObservations;
    private final CompletionDispatcher completionDispatcher;
    private final Map<RuntimeSemanticHoldRef, HoldEntry> activeHolds =
        new LinkedHashMap<>();
    private final Map<RuntimeSemanticHoldRef, HoldEntry> allHolds =
        new LinkedHashMap<>();
    private final Map<RuntimeSemanticPredecessorGuardRef, GuardEntry> guards =
        new LinkedHashMap<>();
    private final Map<RuntimeSemanticPredecessorGuardRef, GuardEntry> allGuards =
        new LinkedHashMap<>();
    private final Set<ConnectionId> failedRequiredObservationConnections =
        new LinkedHashSet<>();
    private long nextHoldValue = FIRST_CONTROL_VALUE;
    private long nextGuardValue = FIRST_CONTROL_VALUE;
    private boolean acceptingNewControls = true;

    SemanticControlCoordinator(
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        SemanticControlCapabilityRegistry controlCapabilities
    ) {
        this(
            events,
            proofSubjects,
            controlCapabilities,
            new SystemTimeoutScheduler(),
            ProofObservationListener.NONE,
            new SystemCompletionDispatcher()
        );
    }

    SemanticControlCoordinator(
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        SemanticControlCapabilityRegistry controlCapabilities,
        ProofObservationListener proofObservations
    ) {
        this(
            events,
            proofSubjects,
            controlCapabilities,
            new SystemTimeoutScheduler(),
            proofObservations,
            new SystemCompletionDispatcher()
        );
    }

    SemanticControlCoordinator(
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        SemanticControlCapabilityRegistry controlCapabilities,
        TimeoutScheduler timeoutScheduler
    ) {
        this(
            events,
            proofSubjects,
            controlCapabilities,
            timeoutScheduler,
            ProofObservationListener.NONE,
            new SystemCompletionDispatcher()
        );
    }

    SemanticControlCoordinator(
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        SemanticControlCapabilityRegistry controlCapabilities,
        TimeoutScheduler timeoutScheduler,
        ProofObservationListener proofObservations
    ) {
        this(
            events,
            proofSubjects,
            controlCapabilities,
            timeoutScheduler,
            proofObservations,
            new SystemCompletionDispatcher()
        );
    }

    SemanticControlCoordinator(
        EnvironmentEventPublisher events,
        ProofSubjectRegistry proofSubjects,
        SemanticControlCapabilityRegistry controlCapabilities,
        TimeoutScheduler timeoutScheduler,
        ProofObservationListener proofObservations,
        CompletionDispatcher completionDispatcher
    ) {
        this.events = Objects.requireNonNull(events, "events must not be null");
        this.proofSubjects = Objects.requireNonNull(
            proofSubjects,
            "proofSubjects must not be null"
        );
        this.controlCapabilities = Objects.requireNonNull(
            controlCapabilities,
            "controlCapabilities must not be null"
        );
        this.timeoutScheduler = Objects.requireNonNull(
            timeoutScheduler,
            "timeoutScheduler must not be null"
        );
        this.proofObservations = Objects.requireNonNull(
            proofObservations,
            "proofObservations must not be null"
        );
        this.completionDispatcher = Objects.requireNonNull(
            completionDispatcher,
            "completionDispatcher must not be null"
        );
    }

    @Override
    public <T> SemanticHold declareHold(
        SemanticInteractionSelector<T> selector,
        Duration maximumHoldDuration
    ) {
        selector = Objects.requireNonNull(selector, "selector must not be null");
        maximumHoldDuration = requirePositive(
            maximumHoldDuration,
            "maximumHoldDuration"
        );
        synchronized (this) {
            requireAccepting();
            requireControlCapacity();
            validateSelector(selector);
            RuntimeSemanticHoldRef ref = nextHoldReference();
            HoldEntry entry = new HoldEntry(ref, selector, maximumHoldDuration);
            allHolds.put(ref, entry);
            return new SemanticHoldHandle(this, entry);
        }
    }

    @Override
    public SemanticPredecessorGuard declareGuard(
        SemanticPredecessorGuardSpec specification
    ) {
        specification = Objects.requireNonNull(
            specification,
            "specification must not be null"
        );
        synchronized (this) {
            requireAccepting();
            requireControlCapacity();
            proofSubjects.validateSubject(specification.subject());
            validateSelector(specification.predecessor().selector());
            validateSelector(specification.successor());
            RuntimeSemanticPredecessorGuardRef ref = nextGuardReference();
            GuardEntry entry = new GuardEntry(ref, specification);
            allGuards.put(ref, entry);
            return new SemanticPredecessorGuardHandle(this, entry);
        }
    }

    @Override
    public <T> SemanticHold arm(
        SemanticInteractionSelector<T> selector,
        Duration maximumHoldDuration
    ) {
        SemanticHold declared = declareHold(selector, maximumHoldDuration);
        activatePreparedControls(List.of(declared.ref()), List.of());
        return declared;
    }

    @Override
    public SemanticPredecessorGuard guard(
        SemanticPredecessorGuardSpec specification
    ) {
        SemanticPredecessorGuard declared = declareGuard(specification);
        activatePreparedControls(List.of(), List.of(declared.ref()));
        return declared;
    }

    void activatePreparedControls(
        List<SemanticHoldRef> holdRefs,
        List<SemanticPredecessorGuardRef> guardRefs
    ) {
        activatePreparedControls(holdRefs, guardRefs, () -> {});
    }

    void activatePreparedControls(
        List<SemanticHoldRef> holdRefs,
        List<SemanticPredecessorGuardRef> guardRefs,
        Runnable activationBoundary
    ) {
        Objects.requireNonNull(activationBoundary, "activationBoundary must not be null");
        activatePreparedControls(
            holdRefs,
            guardRefs,
            () -> {
                activationBoundary.run();
                return ignored -> true;
            }
        );
    }

    void activatePreparedControls(
        List<SemanticHoldRef> holdRefs,
        List<SemanticPredecessorGuardRef> guardRefs,
        Supplier<Predicate<InteractionRef>> evidenceWindowBoundary
    ) {
        holdRefs = List.copyOf(Objects.requireNonNull(holdRefs, "holdRefs must not be null"));
        guardRefs = List.copyOf(Objects.requireNonNull(guardRefs, "guardRefs must not be null"));
        evidenceWindowBoundary = Objects.requireNonNull(
            evidenceWindowBoundary,
            "evidenceWindowBoundary must not be null"
        );
        AfterTransition afterTransition = new AfterTransition();
        Throwable activationFailure = null;
        synchronized (this) {
            requireAccepting();
            List<HoldEntry> holds = distinctHolds(holdRefs);
            List<GuardEntry> preparedGuards = distinctGuards(guardRefs);
            holds.forEach(this::validateCanActivate);
            preparedGuards.forEach(this::validateCanActivate);
            List<HoldEntry> activatedHolds = new ArrayList<>();
            List<GuardEntry> activatedGuards = new ArrayList<>();
            try {
                for (HoldEntry entry : holds) {
                    appendHold(
                        entry,
                        SemanticHoldState.ARMED,
                        Optional.empty(),
                        () -> {
                            entry.state = SemanticHoldState.ARMED;
                            activeHolds.put(entry.ref, entry);
                        }
                    );
                    activatedHolds.add(entry);
                }
                for (GuardEntry entry : preparedGuards) {
                    appendGuardState(
                        entry,
                        SemanticPredecessorGuardState.ARMED,
                        Optional.empty(),
                        () -> {
                            entry.state = SemanticPredecessorGuardState.ARMED;
                            guards.put(entry.ref, entry);
                        }
                    );
                    scheduleGuardTimeout(entry, afterTransition);
                    activatedGuards.add(entry);
                }
                if (holds.stream().anyMatch(entry -> entry.state != SemanticHoldState.ARMED)
                    || preparedGuards.stream().anyMatch(
                        entry -> entry.state != SemanticPredecessorGuardState.ARMED
                    )) {
                    throw new IllegalStateException(
                        "Prepared controls did not remain ARMED at the activation boundary"
                    );
                }
                Predicate<InteractionRef> evidenceWindow = Objects.requireNonNull(
                    evidenceWindowBoundary.get(),
                    "evidenceWindowBoundary must return a membership predicate"
                );
                activatedHolds.forEach(entry -> entry.evidenceWindow = evidenceWindow);
                activatedGuards.forEach(entry -> entry.evidenceWindow = evidenceWindow);
            } catch (RuntimeException | Error failure) {
                for (HoldEntry entry : activatedHolds) {
                    if (entry.state == SemanticHoldState.ARMED) {
                        terminalHoldLocked(
                            entry,
                            SemanticHoldState.CANCELLED,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                }
                for (GuardEntry entry : activatedGuards) {
                    if (guardIsActiveForFailureOrTeardown(entry.state)) {
                        terminalGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.CANCELLED,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                }
                activationFailure = failure;
            }
        }
        runAfterTransition(afterTransition);
        if (activationFailure instanceof RuntimeException failure) {
            throw failure;
        }
        if (activationFailure instanceof Error failure) {
            throw failure;
        }
    }

    void cancelPreparedControls(
        List<SemanticHoldRef> holdRefs,
        List<SemanticPredecessorGuardRef> guardRefs
    ) {
        PreparedControlCancellation cancellation = cancelPreparedControlsInternally(
            holdRefs,
            guardRefs
        );
        runAfterTransition(cancellation.actions());
        cancellation.rethrowFirstFailure();
    }

    PreparedControlCancellation cancelPreparedControlsInternally(
        List<SemanticHoldRef> holdRefs,
        List<SemanticPredecessorGuardRef> guardRefs
    ) {
        AfterTransition afterTransition = new AfterTransition();
        List<Throwable> failures = new ArrayList<>();
        synchronized (this) {
            for (HoldEntry entry : distinctHolds(holdRefs)) {
                if (entry.state == SemanticHoldState.DECLARED
                    || entry.state == SemanticHoldState.ARMED
                    || entry.state == SemanticHoldState.REACHED_HELD) {
                    try {
                        terminalHoldLocked(
                            entry,
                            SemanticHoldState.CANCELLED,
                            Optional.empty(),
                            afterTransition
                        );
                    } catch (RuntimeException | Error failure) {
                        failures.add(failure);
                    }
                }
            }
            for (GuardEntry entry : distinctGuards(guardRefs)) {
                if (entry.state == SemanticPredecessorGuardState.DECLARED
                    || guardIsActiveForFailureOrTeardown(entry.state)) {
                    try {
                        terminalGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.CANCELLED,
                            Optional.empty(),
                            afterTransition
                        );
                    } catch (RuntimeException | Error failure) {
                        failures.add(failure);
                    }
                }
            }
        }
        return new PreparedControlCancellation(afterTransition, failures);
    }

    synchronized HoldDeclaration holdDeclaration(SemanticHoldRef ref) {
        HoldEntry entry = requireHold(ref);
        return new HoldDeclaration(
            entry.ref,
            entry.state,
            entry.connectionId,
            entry.proofSubject
        );
    }

    synchronized GuardDeclaration guardDeclaration(SemanticPredecessorGuardRef ref) {
        GuardEntry entry = requireGuard(ref);
        return new GuardDeclaration(
            entry.ref,
            entry.state,
            entry.subject,
            entry.predecessorSelector.connectionId(),
            entry.successorSelector.connectionId()
        );
    }

    @Override
    public ForwardingPermit permit(RecordedInteraction interaction) {
        interaction = Objects.requireNonNull(interaction, "interaction must not be null");
        RecordedInteraction recorded = interaction;
        AfterTransition afterTransition = new AfterTransition();
        ForwardingPermit permit;
        try {
            synchronized (this) {
                permit = events.proofFactBatch(() -> {
                    GuardSelections guardSelections = selectGuardsLocked(recorded);
                    HoldMatch holdMatch = guardSelections.closesSession
                        ? HoldMatch.none()
                        : selectHoldLocked(recorded);
                    return decideLocked(
                        recorded,
                        guardSelections,
                        holdMatch,
                        afterTransition
                    );
                }, afterTransition::addFinalizationHandoff);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
        return permit;
    }

    @Override
    public void observationFailed(ConnectionId connectionId) {
        connectionId = Objects.requireNonNull(connectionId, "connectionId must not be null");
        ConnectionId failedConnection = connectionId;
        AfterTransition afterTransition = new AfterTransition();
        try {
            synchronized (this) {
                authoritativeOperationLocked(() -> {
                    failedRequiredObservationConnections.add(failedConnection);
                    proofObservations.requiredObservationFailed(failedConnection);
                    for (GuardEntry entry : List.copyOf(guards.values())) {
                        if (guardIsActiveForFailureOrTeardown(entry.state)
                            && entry.concerns(failedConnection)) {
                            failGuardLocked(
                                entry,
                                SemanticPredecessorGuardFailure
                                    .REQUIRED_OBSERVATION_FAILURE,
                                afterTransition
                            );
                        } else if (entry.state == SemanticPredecessorGuardState.VIOLATED
                            && entry.concerns(failedConnection)) {
                            appendGuardSuppressedFailure(
                                entry,
                                SemanticPredecessorGuardFailure
                                    .REQUIRED_OBSERVATION_FAILURE
                            );
                        }
                    }
                    return null;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
    }

    void withRequiredObservationBoundary(Runnable action) {
        Objects.requireNonNull(action, "action must not be null");
        synchronized (this) {
            action.run();
        }
    }

    private ForwardingPermit decideLocked(
        RecordedInteraction interaction,
        GuardSelections guardSelections,
        HoldMatch holdMatch,
        AfterTransition afterTransition
    ) {
        GuardDecision guardDecision = commitGuardSelectionsLocked(
            interaction,
            guardSelections,
            afterTransition
        );
        List<GuardUse> forwardedPredecessors = guardDecision.forwardedPredecessors;
        if (guardDecision.closeSession) {
            abortGuardUsesLocked(
                forwardedPredecessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            abortGuardUsesLocked(
                guardDecision.authorizedSuccessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            return CLOSE_SESSION;
        }

        if (holdMatch.failedClosed) {
            for (HoldEntry failed : holdMatch.failedEntries) {
                failHoldLocked(failed, holdMatch.failure, afterTransition);
            }
            abortGuardUsesLocked(
                forwardedPredecessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            abortGuardUsesLocked(
                guardDecision.authorizedSuccessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            return CLOSE_SESSION;
        }

        HoldEntry held = holdMatch.entry;
        if (held != null) {
            reachHoldLocked(held, holdMatch.selection, interaction, afterTransition);
            if (held.state != SemanticHoldState.REACHED_HELD) {
                abortGuardUsesLocked(
                    forwardedPredecessors,
                    SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                    afterTransition
                );
                abortGuardUsesLocked(
                    guardDecision.authorizedSuccessors,
                    SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                    afterTransition
                );
                return CLOSE_SESSION;
            }
        }

        if (held == null
            && forwardedPredecessors.isEmpty()
            && guardDecision.authorizedSuccessors.isEmpty()) {
            return IMMEDIATE_FORWARD;
        }
        PermitContext context = new PermitContext(
            held,
            List.copyOf(forwardedPredecessors),
            List.copyOf(guardDecision.authorizedSuccessors)
        );
        CoordinatedPermit permit = new CoordinatedPermit(this, context);
        context.permit = permit;
        if (held != null) {
            held.permit = permit;
        } else {
            permit.authorize(ForwardingDecision.FORWARD);
        }
        return permit;
    }

    private GuardSelections selectGuardsLocked(RecordedInteraction interaction) {
        List<GuardInteractionSelection> selections = new ArrayList<>();
        boolean closeSession = false;
        for (GuardEntry entry : guards.values()) {
            SemanticPredecessorGuardState stateBeforeInteraction = entry.state;
            if (!guardEnforcesLaterTarget(entry)) {
                continue;
            }
            if (!entry.evidenceWindow.test(interaction.interactionRef())) {
                continue;
            }
            SelectorSelection predecessor = null;
            boolean predecessorFailed = false;
            if (stateBeforeInteraction == SemanticPredecessorGuardState.ARMED) {
                try {
                    predecessor = select(entry.predecessorSelector, interaction);
                } catch (RuntimeException | Error failure) {
                    predecessorFailed = true;
                }
            }
            SelectorSelection successor = null;
            boolean successorFailed = false;
            try {
                successor = select(entry.successorSelector, interaction);
            } catch (RuntimeException | Error failure) {
                successorFailed = true;
            }
            GuardInteractionSelection selection = new GuardInteractionSelection(
                entry,
                stateBeforeInteraction,
                predecessor,
                predecessorFailed,
                successor,
                successorFailed
            );
            selections.add(selection);
            closeSession |= selection.closesSession(interaction.interactionRef());
        }
        return new GuardSelections(List.copyOf(selections), closeSession);
    }

    private GuardDecision commitGuardSelectionsLocked(
        RecordedInteraction interaction,
        GuardSelections guardSelections,
        AfterTransition afterTransition
    ) {
        List<GuardUse> forwardedPredecessors = new ArrayList<>();
        List<GuardUse> authorized = new ArrayList<>();
        boolean close = false;
        for (GuardInteractionSelection selection : guardSelections.selections) {
            if (!selection.predecessorFailed && !selection.successorFailed) {
                continue;
            }
            failGuardLocked(
                selection.entry,
                SemanticPredecessorGuardFailure.SELECTOR_EVALUATION,
                afterTransition
            );
        }
        for (GuardInteractionSelection interactionSelection : guardSelections.selections) {
            GuardEntry entry = interactionSelection.entry;
            if (interactionSelection.predecessorFailed
                || interactionSelection.successorFailed) {
                close = true;
                continue;
            }
            SelectorSelection selection = interactionSelection.successor;
            if (selection == null) {
                continue;
            }
            InteractionRef successorRef = interaction.interactionRef();
            if (interactionSelection.stateBeforeInteraction
                == SemanticPredecessorGuardState.ARMED) {
                terminalGuardLocked(
                    entry,
                    SemanticPredecessorGuardState.VIOLATED,
                    Optional.empty(),
                    Optional.ofNullable(entry.predecessor),
                    Optional.of(successorRef),
                    () -> {
                        entry.successor = successorRef;
                        entry.successorSelection = selection;
                    },
                    afterTransition
                );
                close = true;
                continue;
            }
            switch (entry.state) {
                case PREDECESSOR_SATISFIED -> {
                    if (successorRef.equals(entry.predecessor)) {
                        terminalGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.VIOLATED,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(successorRef),
                            () -> {
                                entry.predecessor = null;
                                entry.predecessorSelection = null;
                                entry.successor = successorRef;
                                entry.successorSelection = selection;
                            },
                            afterTransition
                        );
                        close = true;
                        continue;
                    }
                    transitionGuardLocked(
                        entry,
                        SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED,
                        Optional.empty(),
                        Optional.ofNullable(entry.predecessor),
                        Optional.of(successorRef),
                        () -> {
                            entry.successor = successorRef;
                            entry.successorSelection = selection;
                        }
                    );
                    cancelTimeout(entry);
                    appendGuardDecision(entry, ForwardingDecision.FORWARD);
                    authorized.add(new GuardUse(entry, selection));
                }
                case ARMED, PREDECESSOR_OBSERVED -> {
                    terminalGuardLocked(
                        entry,
                        SemanticPredecessorGuardState.VIOLATED,
                        Optional.empty(),
                        Optional.ofNullable(entry.predecessor),
                        Optional.of(successorRef),
                        () -> {
                            entry.successor = successorRef;
                            entry.successorSelection = selection;
                        },
                        afterTransition
                    );
                    close = true;
                }
                case VIOLATED, CANCELLED, TIMED_OUT, FAILED -> {
                    appendGuardDecision(
                        entry,
                        ForwardingDecision.CLOSE_SESSION,
                        Optional.ofNullable(entry.predecessor),
                        Optional.of(successorRef),
                        () -> {
                            entry.successor = successorRef;
                            entry.successorSelection = selection;
                        }
                    );
                    close = true;
                }
                default -> throw new IllegalStateException(
                    "Unexpected enforcing guard state " + entry.state
                );
            }
        }
        for (GuardInteractionSelection selection : guardSelections.selections) {
            GuardEntry entry = selection.entry;
            if (selection.predecessor == null
                || selection.predecessorFailed
                || entry.state != SemanticPredecessorGuardState.ARMED) {
                continue;
            }
            InteractionRef predecessorRef = interaction.interactionRef();
            SelectorSelection predecessorSelection = selection.predecessor;
            if (entry.requiredBoundary == SemanticPredecessorBoundary.CONFIRMED) {
                transitionGuardLocked(
                    entry,
                    SemanticPredecessorGuardState.PREDECESSOR_SATISFIED,
                    Optional.empty(),
                    Optional.of(predecessorRef),
                    Optional.ofNullable(entry.successor),
                    () -> {
                        entry.predecessor = predecessorRef;
                        entry.predecessorSelection = predecessorSelection;
                    }
                );
            } else {
                transitionGuardLocked(
                    entry,
                    SemanticPredecessorGuardState.PREDECESSOR_OBSERVED,
                    Optional.empty(),
                    Optional.of(predecessorRef),
                    Optional.ofNullable(entry.successor),
                    () -> {
                        entry.predecessor = predecessorRef;
                        entry.predecessorSelection = predecessorSelection;
                    }
                );
                forwardedPredecessors.add(new GuardUse(entry, predecessorSelection));
            }
        }
        return new GuardDecision(close, forwardedPredecessors, authorized);
    }

    private HoldMatch selectHoldLocked(
        RecordedInteraction interaction
    ) {
        List<HoldSelection> matches = new ArrayList<>();
        for (HoldEntry entry : activeHolds.values()) {
            if (entry.state != SemanticHoldState.ARMED) {
                continue;
            }
            if (!entry.evidenceWindow.test(interaction.interactionRef())) {
                continue;
            }
            SelectorSelection selection;
            try {
                selection = select(entry.selector, interaction);
            } catch (RuntimeException | Error failure) {
                return HoldMatch.failed(
                    List.of(entry),
                    SemanticHoldFailure.SELECTOR_EVALUATION
                );
            }
            if (selection != null) {
                matches.add(new HoldSelection(entry, selection));
            }
        }
        if (matches.size() > 1) {
            return HoldMatch.failed(
                matches.stream().map(HoldSelection::entry).toList(),
                SemanticHoldFailure.AMBIGUOUS_MATCH
            );
        }
        if (matches.isEmpty()) {
            return HoldMatch.none();
        }
        HoldSelection selected = matches.getFirst();
        return new HoldMatch(
            selected.entry,
            selected.selection,
            false,
            List.of(),
            null
        );
    }

    private void reachHoldLocked(
        HoldEntry entry,
        SelectorSelection selection,
        RecordedInteraction interaction,
        AfterTransition afterTransition
    ) {
        InteractionRef interactionRef = interaction.interactionRef();
        appendHold(
            entry,
            SemanticHoldState.REACHED_HELD,
            Optional.empty(),
            Optional.of(interactionRef),
            () -> {
                entry.interactionRef = interactionRef;
                entry.selection = selection;
                entry.state = SemanticHoldState.REACHED_HELD;
                entry.reachedEstablished = true;
            }
        );
        afterTransition.addPublic(() -> entry.reached.complete(entry.interactionRef));
        try {
            TimeoutTask scheduled = timeoutScheduler.schedule(
                entry.maximumHoldDuration,
                () -> timeout(entry)
            );
            if (entry.state == SemanticHoldState.REACHED_HELD) {
                entry.timeoutTask = scheduled;
            } else {
                scheduled.cancel();
            }
        } catch (RuntimeException | Error schedulingFailure) {
            failHoldLocked(entry, SemanticHoldFailure.INTERNAL_FAILURE, afterTransition);
        }
    }

    private SelectorSelection select(
        SemanticInteractionSelector<?> selector,
        RecordedInteraction interaction
    ) {
        InteractionRef reference = interaction.interactionRef();
        if (!selector.connectionId().equals(reference.connectionId())
            || selector.direction() != reference.direction()
            || !selector.evidenceSchema().equals(interaction.evidence().schemaId())
            || !matches(selector, interaction.evidence())) {
            return null;
        }
        if (selector.proofSubject().isEmpty()) {
            return new SelectorSelection(selector, reference, null);
        }
        ProofSubjectRef subject = selector.proofSubject().orElseThrow();
        Optional<CorrelationKey> nativeFlowKey = selector.nativeFlowCorrelationKey();
        if (nativeFlowKey.isEmpty()) {
            return proofSubjects.isSoleUniqueSubjectFor(subject, reference)
                ? new SelectorSelection(selector, reference, null)
                : null;
        }
        Optional<NativeFlowResolution> resolved = proofSubjects.soleUniqueNativeFlow(
            subject,
            nativeFlowKey.orElseThrow(),
            selector.nativeFlowReferenceSchema().orElseThrow()
        );
        if (resolved.isEmpty()) {
            return null;
        }
        NativeFlowResolution nativeFlow = resolved.orElseThrow();
        if (!nativeFlow.containsCandidate(reference)
            || !matchesNativeFlow(
                selector,
                interaction.evidence(),
                nativeFlow.nativeReference()
            )
            || !proofSubjects.remainsSoleUniqueNativeFlow(nativeFlow)) {
            return null;
        }
        return new SelectorSelection(selector, reference, nativeFlow);
    }

    private static <T> boolean matches(
        SemanticInteractionSelector<T> selector,
        EvidenceSnapshot evidence
    ) {
        return selector.matches(evidence.decode(selector.evidenceCodec()));
    }

    private static <T> boolean matchesNativeFlow(
        SemanticInteractionSelector<T> selector,
        EvidenceSnapshot evidence,
        EvidenceSnapshot nativeReference
    ) {
        Object resolved = nativeReference.decode(
            selector.nativeFlowReferenceCodec().orElseThrow()
        );
        return selector.matchesNativeFlow(
            evidence.decode(selector.evidenceCodec()),
            resolved
        );
    }

    private void validateSelector(SemanticInteractionSelector<?> selector) {
        controlCapabilities.validateSelector(selector);
        selector.proofSubject().ifPresent(proofSubjects::validateSubject);
        selector.nativeFlowCorrelationKey().ifPresent(key ->
            proofSubjects.validateSubjectFlow(
                selector.proofSubject().orElseThrow(),
                key
            )
        );
    }

    private SemanticHoldState state(HoldEntry entry) {
        synchronized (this) {
            return entry.state;
        }
    }

    private SemanticPredecessorGuardState state(GuardEntry entry) {
        synchronized (this) {
            return entry.state;
        }
    }

    private CompletionStage<Void> release(HoldEntry entry) {
        AfterTransition afterTransition = new AfterTransition();
        CompletionStage<Void> result;
        try {
            synchronized (this) {
                result = authoritativeOperationLocked(() -> {
                    if (entry.state != SemanticHoldState.REACHED_HELD) {
                        return failedStage(
                            "Semantic hold cannot be released from state " + entry.state
                        );
                    }
                    CompletionStage<Void> release =
                        entry.releaseCompletion.minimalCompletionStage();
                    if (!entry.selection.remainsValid(proofSubjects)) {
                        failHoldLocked(
                            entry,
                            SemanticHoldFailure.CORRELATION_INVALIDATED,
                            afterTransition
                        );
                    } else {
                        transitionHoldLocked(
                            entry,
                            SemanticHoldState.RELEASING,
                            Optional.empty()
                        );
                        cancelTimeout(entry);
                        afterTransition.addInternal(
                            () -> entry.permit.authorize(ForwardingDecision.FORWARD)
                        );
                    }
                    return release;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
        return result;
    }

    private boolean cancel(HoldEntry entry) {
        AfterTransition afterTransition = new AfterTransition();
        boolean cancelled;
        try {
            synchronized (this) {
                cancelled = authoritativeOperationLocked(() -> {
                    if (entry.state != SemanticHoldState.DECLARED
                        && entry.state != SemanticHoldState.ARMED
                        && entry.state != SemanticHoldState.REACHED_HELD) {
                        return false;
                    }
                    terminalHoldLocked(
                        entry,
                        SemanticHoldState.CANCELLED,
                        Optional.empty(),
                        afterTransition
                    );
                    return true;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
        return cancelled;
    }

    private boolean cancel(GuardEntry entry) {
        AfterTransition afterTransition = new AfterTransition();
        boolean cancelled;
        try {
            synchronized (this) {
                cancelled = authoritativeOperationLocked(() -> {
                    if (entry.state != SemanticPredecessorGuardState.DECLARED
                        && !guardAwaitsTimedBoundary(entry.state)) {
                        return false;
                    }
                    terminalGuardLocked(
                        entry,
                        SemanticPredecessorGuardState.CANCELLED,
                        Optional.empty(),
                        afterTransition
                    );
                    return true;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
        return cancelled;
    }

    private void timeout(HoldEntry entry) {
        AfterTransition afterTransition = new AfterTransition();
        try {
            synchronized (this) {
                authoritativeOperationLocked(() -> {
                    if (entry.state == SemanticHoldState.REACHED_HELD) {
                        terminalHoldLocked(
                            entry,
                            SemanticHoldState.TIMED_OUT,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                    return null;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
    }

    private void timeout(GuardEntry entry) {
        AfterTransition afterTransition = new AfterTransition();
        try {
            synchronized (this) {
                authoritativeOperationLocked(() -> {
                    if (guardAwaitsTimedBoundary(entry.state)) {
                        terminalGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.TIMED_OUT,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                    return null;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
    }

    private void forwarded(PermitContext context) {
        AfterTransition afterTransition = new AfterTransition();
        try {
            synchronized (this) {
                authoritativeOperationLocked(() -> {
                    if (!context.claimOutcome()) {
                        return null;
                    }
                    for (GuardUse use : context.authorizedSuccessors) {
                        GuardEntry entry = use.entry;
                        if (entry.state
                            != SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED) {
                            continue;
                        }
                        if (!use.selection.remainsValid(proofSubjects)) {
                            failGuardLocked(
                                entry,
                                SemanticPredecessorGuardFailure.CORRELATION_INVALIDATED,
                                afterTransition
                            );
                            continue;
                        }
                        terminalGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.SATISFIED,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                    for (GuardUse use : context.forwardedPredecessors) {
                        GuardEntry entry = use.entry;
                        if (entry.state
                            != SemanticPredecessorGuardState.PREDECESSOR_OBSERVED) {
                            continue;
                        }
                        if (!use.selection.remainsValid(proofSubjects)) {
                            failGuardLocked(
                                entry,
                                SemanticPredecessorGuardFailure.CORRELATION_INVALIDATED,
                                afterTransition
                            );
                            continue;
                        }
                        transitionGuardLocked(
                            entry,
                            SemanticPredecessorGuardState.PREDECESSOR_SATISFIED,
                            Optional.empty()
                        );
                    }
                    if (context.hold != null
                        && context.hold.state == SemanticHoldState.RELEASING) {
                        terminalHoldLocked(
                            context.hold,
                            SemanticHoldState.FORWARDED,
                            Optional.empty(),
                            afterTransition
                        );
                    }
                    return null;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
    }

    private void writeFailed(PermitContext context) {
        failPermit(context, SemanticPredecessorGuardFailure.WRITE_FAILURE,
            SemanticHoldFailure.WRITE_FAILURE);
    }

    private void abandoned(PermitContext context) {
        failPermit(context, SemanticPredecessorGuardFailure.SESSION_ABANDONED,
            SemanticHoldFailure.SESSION_ABANDONED);
    }

    private void failPermit(
        PermitContext context,
        SemanticPredecessorGuardFailure guardFailure,
        SemanticHoldFailure holdFailure
    ) {
        AfterTransition afterTransition = new AfterTransition();
        try {
            synchronized (this) {
                authoritativeOperationLocked(() -> {
                    if (!context.claimOutcome()) {
                        return null;
                    }
                    abortGuardUsesLocked(
                        context.authorizedSuccessors,
                        guardFailure,
                        afterTransition
                    );
                    abortGuardUsesLocked(
                        context.forwardedPredecessors,
                        guardFailure,
                        afterTransition
                    );
                    if (context.hold != null
                        && (context.hold.state == SemanticHoldState.REACHED_HELD
                            || context.hold.state == SemanticHoldState.RELEASING)) {
                        failHoldLocked(context.hold, holdFailure, afterTransition);
                    }
                    return null;
                }, afterTransition);
            }
        } finally {
            runAfterTransition(afterTransition);
        }
    }

    void completeExecution() {
        AfterTransition afterTransition = new AfterTransition();
        synchronized (this) {
            if (!acceptingNewControls) {
                return;
            }
            acceptingNewControls = false;
            for (HoldEntry entry : List.copyOf(allHolds.values())) {
                if (entry.state == SemanticHoldState.DECLARED
                    || entry.state == SemanticHoldState.ARMED
                    || entry.state == SemanticHoldState.REACHED_HELD) {
                    terminalHoldLocked(
                        entry,
                        SemanticHoldState.CANCELLED,
                        Optional.empty(),
                        afterTransition
                    );
                }
            }
            for (GuardEntry entry : List.copyOf(allGuards.values())) {
                if (entry.state == SemanticPredecessorGuardState.DECLARED
                    || guardIsActiveForFailureOrTeardown(entry.state)) {
                    terminalGuardLocked(
                        entry,
                        SemanticPredecessorGuardState.CANCELLED,
                        Optional.empty(),
                        Optional.ofNullable(entry.predecessor),
                        Optional.ofNullable(entry.successor),
                        () -> entry.retainCancelledEnforcement = true,
                        afterTransition
                    );
                }
            }
        }
        runAfterTransition(afterTransition);
        try {
            timeoutScheduler.close();
        } finally {
            closeCompletionDispatcherSafely();
        }
    }

    private void abortGuardUsesLocked(
        List<GuardUse> uses,
        SemanticPredecessorGuardFailure failure,
        AfterTransition afterTransition
    ) {
        for (GuardUse use : uses) {
            GuardEntry entry = use.entry;
            if (entry.state == SemanticPredecessorGuardState.PREDECESSOR_OBSERVED
                || entry.state == SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED) {
                failGuardLocked(entry, failure, afterTransition);
            }
        }
    }

    private void failHoldLocked(
        HoldEntry entry,
        SemanticHoldFailure failure,
        AfterTransition afterTransition
    ) {
        terminalHoldLocked(
            entry,
            SemanticHoldState.FAILED,
            Optional.of(Objects.requireNonNull(failure, "failure must not be null")),
            afterTransition
        );
    }

    private void terminalHoldLocked(
        HoldEntry entry,
        SemanticHoldState terminalState,
        Optional<SemanticHoldFailure> failure,
        AfterTransition afterTransition
    ) {
        transitionHoldLocked(entry, terminalState, failure);
        cancelTimeout(entry);
        activeHolds.remove(entry.ref);
        entry.selector = null;
        entry.selection = null;
        if (entry.permit != null && terminalState != SemanticHoldState.FORWARDED) {
            abortGuardUsesLocked(
                entry.permit.context.forwardedPredecessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            abortGuardUsesLocked(
                entry.permit.context.authorizedSuccessors,
                SemanticPredecessorGuardFailure.SESSION_ABANDONED,
                afterTransition
            );
            afterTransition.addInternal(
                () -> entry.permit.authorize(ForwardingDecision.CLOSE_SESSION)
            );
        }
        IllegalStateException terminalFailure = terminalFailure("hold", entry.state);
        if (!entry.reachedEstablished && !entry.reached.isDone()) {
            afterTransition.addPublic(
                () -> entry.reached.completeExceptionally(terminalFailure)
            );
        }
        if (terminalState == SemanticHoldState.FORWARDED) {
            afterTransition.addPublic(() -> entry.releaseCompletion.complete(null));
        } else if (!entry.releaseCompletion.isDone()) {
            afterTransition.addPublic(
                () -> entry.releaseCompletion.completeExceptionally(terminalFailure)
            );
        }
        afterTransition.addPublic(() -> entry.completion.complete(terminalState));
    }

    private void transitionHoldLocked(
        HoldEntry entry,
        SemanticHoldState next,
        Optional<SemanticHoldFailure> failure
    ) {
        next = Objects.requireNonNull(next, "next must not be null");
        SemanticHoldState committedState = next;
        appendHold(entry, next, failure, () -> entry.state = committedState);
    }

    private void failGuardLocked(
        GuardEntry entry,
        SemanticPredecessorGuardFailure failure,
        AfterTransition afterTransition
    ) {
        if (!guardIsActiveForFailureOrTeardown(entry.state)) {
            return;
        }
        terminalGuardLocked(
            entry,
            SemanticPredecessorGuardState.FAILED,
            Optional.of(Objects.requireNonNull(failure, "failure must not be null")),
            afterTransition
        );
    }

    private void terminalGuardLocked(
        GuardEntry entry,
        SemanticPredecessorGuardState terminalState,
        Optional<SemanticPredecessorGuardFailure> failure,
        AfterTransition afterTransition
    ) {
        terminalGuardLocked(
            entry,
            terminalState,
            failure,
            Optional.ofNullable(entry.predecessor),
            Optional.ofNullable(entry.successor),
            () -> {},
            afterTransition
        );
    }

    private void terminalGuardLocked(
        GuardEntry entry,
        SemanticPredecessorGuardState terminalState,
        Optional<SemanticPredecessorGuardFailure> failure,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Runnable stateCommit,
        AfterTransition afterTransition
    ) {
        transitionGuardLocked(
            entry,
            terminalState,
            failure,
            predecessor,
            successor,
            stateCommit
        );
        cancelTimeout(entry);
        if (terminalState == SemanticPredecessorGuardState.SATISFIED
            || (terminalState == SemanticPredecessorGuardState.CANCELLED
                && !entry.retainCancelledEnforcement)) {
            guards.remove(entry.ref);
        }
        if (!entry.completion.isDone()) {
            afterTransition.addPublic(() -> entry.completion.complete(terminalState));
        }
    }

    private void transitionGuardLocked(
        GuardEntry entry,
        SemanticPredecessorGuardState next,
        Optional<SemanticPredecessorGuardFailure> failure
    ) {
        transitionGuardLocked(
            entry,
            next,
            failure,
            Optional.ofNullable(entry.predecessor),
            Optional.ofNullable(entry.successor),
            () -> {}
        );
    }

    private void transitionGuardLocked(
        GuardEntry entry,
        SemanticPredecessorGuardState next,
        Optional<SemanticPredecessorGuardFailure> failure,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Runnable stateCommit
    ) {
        next = Objects.requireNonNull(next, "next must not be null");
        SemanticPredecessorGuardState committedState = next;
        Runnable committedMutation = () -> {
            stateCommit.run();
            entry.state = committedState;
        };
        if (next == SemanticPredecessorGuardState.SATISFIED) {
            appendGuardFact(
                entry,
                next,
                SemanticPredecessorGuardEvent.Kind.TERMINAL,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                predecessor,
                successor,
                committedMutation
            );
        } else if (next == SemanticPredecessorGuardState.VIOLATED) {
            appendGuardFact(
                entry,
                next,
                SemanticPredecessorGuardEvent.Kind.TERMINAL,
                Optional.of(ForwardingDecision.CLOSE_SESSION),
                Optional.of(SemanticPredecessorViolation.PREDECESSOR_NOT_ESTABLISHED),
                Optional.empty(),
                predecessor,
                successor,
                committedMutation
            );
        } else {
            appendGuardFact(
                entry,
                next,
                SemanticPredecessorGuardEvent.Kind.STATE,
                Optional.empty(),
                Optional.empty(),
                failure,
                predecessor,
                successor,
                committedMutation
            );
        }
    }

    private void appendHold(
        HoldEntry entry,
        SemanticHoldState state,
        Optional<SemanticHoldFailure> failure,
        Runnable stateCommit
    ) {
        appendHold(
            entry,
            state,
            failure,
            Optional.ofNullable(entry.interactionRef),
            stateCommit
        );
    }

    private void appendHold(
        HoldEntry entry,
        SemanticHoldState state,
        Optional<SemanticHoldFailure> failure,
        Optional<InteractionRef> interactionRef,
        Runnable stateCommit
    ) {
        events.semanticHold(
            entry.ref,
            state,
            entry.connectionId,
            entry.direction,
            entry.evidenceSchema,
            entry.proofSubject,
            interactionRef,
            failure,
            stateCommit
        );
    }

    private void appendGuardState(
        GuardEntry entry,
        SemanticPredecessorGuardState state,
        Optional<SemanticPredecessorGuardFailure> failure,
        Runnable stateCommit
    ) {
        appendGuardFact(
            entry,
            state,
            SemanticPredecessorGuardEvent.Kind.STATE,
            Optional.empty(),
            Optional.empty(),
            failure,
            stateCommit
        );
    }

    private void appendGuardDecision(
        GuardEntry entry,
        ForwardingDecision decision
    ) {
        appendGuardDecision(
            entry,
            decision,
            Optional.ofNullable(entry.predecessor),
            Optional.ofNullable(entry.successor),
            () -> {}
        );
    }

    private void appendGuardDecision(
        GuardEntry entry,
        ForwardingDecision decision,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Runnable stateCommit
    ) {
        appendGuardFact(
            entry,
            entry.state,
            SemanticPredecessorGuardEvent.Kind.DECISION,
            Optional.of(decision),
            Optional.empty(),
            Optional.empty(),
            predecessor,
            successor,
            stateCommit
        );
    }

    private void appendGuardSuppressedFailure(
        GuardEntry entry,
        SemanticPredecessorGuardFailure failure
    ) {
        appendGuardFact(
            entry,
            entry.state,
            SemanticPredecessorGuardEvent.Kind.SUPPRESSED_FAILURE,
            Optional.empty(),
            Optional.empty(),
            Optional.of(Objects.requireNonNull(failure, "failure must not be null")),
            () -> {}
        );
    }

    private void appendGuardFact(
        GuardEntry entry,
        SemanticPredecessorGuardState state,
        SemanticPredecessorGuardEvent.Kind kind,
        Optional<ForwardingDecision> decision,
        Optional<SemanticPredecessorViolation> violation,
        Optional<SemanticPredecessorGuardFailure> failure,
        Runnable stateCommit
    ) {
        appendGuardFact(
            entry,
            state,
            kind,
            decision,
            violation,
            failure,
            Optional.ofNullable(entry.predecessor),
            Optional.ofNullable(entry.successor),
            stateCommit
        );
    }

    private void appendGuardFact(
        GuardEntry entry,
        SemanticPredecessorGuardState state,
        SemanticPredecessorGuardEvent.Kind kind,
        Optional<ForwardingDecision> decision,
        Optional<SemanticPredecessorViolation> violation,
        Optional<SemanticPredecessorGuardFailure> failure,
        Optional<InteractionRef> predecessor,
        Optional<InteractionRef> successor,
        Runnable stateCommit
    ) {
        events.semanticPredecessorGuard(
            entry.ref,
            kind,
            entry.subject,
            state,
            entry.requiredBoundary,
            predecessor,
            successor,
            decision,
            violation,
            failure,
            stateCommit
        );
    }

    private RuntimeSemanticHoldRef nextHoldReference() {
        if (nextHoldValue < FIRST_CONTROL_VALUE) {
            throw exhausted("Semantic-hold");
        }
        RuntimeSemanticHoldRef ref = new RuntimeSemanticHoldRef(holdOwner, nextHoldValue);
        nextHoldValue = increment(nextHoldValue);
        return ref;
    }

    private RuntimeSemanticPredecessorGuardRef nextGuardReference() {
        if (nextGuardValue < FIRST_CONTROL_VALUE) {
            throw exhausted("Semantic-predecessor-guard");
        }
        RuntimeSemanticPredecessorGuardRef ref =
            new RuntimeSemanticPredecessorGuardRef(guardOwner, nextGuardValue);
        nextGuardValue = increment(nextGuardValue);
        return ref;
    }

    private void requireAccepting() {
        if (!acceptingNewControls) {
            throw new IllegalStateException(
                "Environment execution is complete and cannot arm semantic controls"
            );
        }
    }

    private void requireControlCapacity() {
        if (allHolds.size() + allGuards.size() >= MAXIMUM_CONTROLS) {
            throw new IllegalStateException(
                "An environment execution supports at most " + MAXIMUM_CONTROLS
                    + " semantic controls"
            );
        }
    }

    private void requireRequiredObservationAvailable(ConnectionId connectionId) {
        if (failedRequiredObservationConnections.contains(connectionId)) {
            throw new IllegalStateException(
                "Connection '" + connectionId
                    + "' has terminal required-observation failure"
            );
        }
    }

    private void validateCanActivate(HoldEntry entry) {
        if (entry.state != SemanticHoldState.DECLARED) {
            throw new IllegalStateException(
                "Semantic hold is not a prepared DECLARED control"
            );
        }
        validateSelector(entry.selector);
        requireRequiredObservationAvailable(entry.connectionId);
    }

    private void validateCanActivate(GuardEntry entry) {
        if (entry.state != SemanticPredecessorGuardState.DECLARED) {
            throw new IllegalStateException(
                "Semantic predecessor guard is not a prepared DECLARED control"
            );
        }
        validateSelector(entry.predecessorSelector);
        validateSelector(entry.successorSelector);
        requireRequiredObservationAvailable(entry.predecessorSelector.connectionId());
        requireRequiredObservationAvailable(entry.successorSelector.connectionId());
    }

    private void scheduleGuardTimeout(
        GuardEntry entry,
        AfterTransition afterTransition
    ) {
        try {
            TimeoutTask scheduled = timeoutScheduler.schedule(
                entry.maximumDuration,
                () -> timeout(entry)
            );
            if (guardAwaitsTimedBoundary(entry.state)) {
                entry.timeoutTask = scheduled;
            } else {
                scheduled.cancel();
            }
        } catch (RuntimeException | Error schedulingFailure) {
            failGuardLocked(
                entry,
                SemanticPredecessorGuardFailure.INTERNAL_FAILURE,
                afterTransition
            );
        }
    }

    private List<HoldEntry> distinctHolds(List<SemanticHoldRef> refs) {
        Set<RuntimeSemanticHoldRef> identities = new LinkedHashSet<>();
        List<HoldEntry> entries = new ArrayList<>();
        for (SemanticHoldRef ref : Objects.requireNonNull(refs, "refs must not be null")) {
            HoldEntry entry = requireHold(ref);
            if (!identities.add(entry.ref)) {
                throw new IllegalArgumentException(
                    "Semantic hold was declared more than once for activation"
                );
            }
            entries.add(entry);
        }
        return entries;
    }

    private List<GuardEntry> distinctGuards(List<SemanticPredecessorGuardRef> refs) {
        Set<RuntimeSemanticPredecessorGuardRef> identities = new LinkedHashSet<>();
        List<GuardEntry> entries = new ArrayList<>();
        for (SemanticPredecessorGuardRef ref : Objects.requireNonNull(
            refs,
            "refs must not be null"
        )) {
            GuardEntry entry = requireGuard(ref);
            if (!identities.add(entry.ref)) {
                throw new IllegalArgumentException(
                    "Semantic predecessor guard was declared more than once for activation"
                );
            }
            entries.add(entry);
        }
        return entries;
    }

    private HoldEntry requireHold(SemanticHoldRef ref) {
        Objects.requireNonNull(ref, "ref must not be null");
        if (!(ref instanceof RuntimeSemanticHoldRef runtimeRef)
            || runtimeRef.owner() != holdOwner) {
            throw new IllegalArgumentException(
                "Semantic hold belongs to a different environment execution"
            );
        }
        HoldEntry entry = allHolds.get(runtimeRef);
        if (entry == null) {
            throw new IllegalArgumentException(
                "Semantic hold is not declared by this environment execution"
            );
        }
        return entry;
    }

    private GuardEntry requireGuard(SemanticPredecessorGuardRef ref) {
        Objects.requireNonNull(ref, "ref must not be null");
        if (!(ref instanceof RuntimeSemanticPredecessorGuardRef runtimeRef)
            || runtimeRef.owner() != guardOwner) {
            throw new IllegalArgumentException(
                "Semantic predecessor guard belongs to a different environment execution"
            );
        }
        GuardEntry entry = allGuards.get(runtimeRef);
        if (entry == null) {
            throw new IllegalArgumentException(
                "Semantic predecessor guard is not declared by this environment execution"
            );
        }
        return entry;
    }

    private static boolean guardAwaitsTimedBoundary(
        SemanticPredecessorGuardState state
    ) {
        return state == SemanticPredecessorGuardState.ARMED
            || state == SemanticPredecessorGuardState.PREDECESSOR_OBSERVED
            || state == SemanticPredecessorGuardState.PREDECESSOR_SATISFIED;
    }

    private static boolean guardIsActiveForFailureOrTeardown(
        SemanticPredecessorGuardState state
    ) {
        return guardAwaitsTimedBoundary(state)
            || state == SemanticPredecessorGuardState.SUCCESSOR_AUTHORIZED;
    }

    private static boolean guardEnforcesLaterTarget(GuardEntry entry) {
        return guardAwaitsTimedBoundary(entry.state)
            || entry.state == SemanticPredecessorGuardState.VIOLATED
            || entry.state == SemanticPredecessorGuardState.TIMED_OUT
            || entry.state == SemanticPredecessorGuardState.FAILED
            || (entry.state == SemanticPredecessorGuardState.CANCELLED
                && entry.retainCancelledEnforcement);
    }

    private static void cancelTimeout(HoldEntry entry) {
        if (entry.timeoutTask != null) {
            entry.timeoutTask.cancel();
            entry.timeoutTask = null;
        }
    }

    private static void cancelTimeout(GuardEntry entry) {
        if (entry.timeoutTask != null) {
            entry.timeoutTask.cancel();
            entry.timeoutTask = null;
        }
    }

    private static IllegalStateException terminalFailure(String kind, Object state) {
        return new IllegalStateException("Semantic " + kind + " completed with state " + state);
    }

    private static CompletionStage<Void> failedStage(String message) {
        return CompletableFuture.<Void>failedFuture(new IllegalStateException(message))
            .minimalCompletionStage();
    }

    private static Duration requirePositive(Duration value, String description) {
        value = Objects.requireNonNull(value, description + " must not be null");
        if (value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(description + " must be positive");
        }
        return value;
    }

    private static long increment(long value) {
        return value == Long.MAX_VALUE ? Long.MIN_VALUE : value + 1L;
    }

    private static IllegalStateException exhausted(String kind) {
        return new IllegalStateException(
            kind + " identity space is exhausted for this environment execution"
        );
    }

    private void runAfterTransition(AfterTransition actions) {
        CompletionGate publicationGate = new CompletionGate();
        try {
            actions.runInternal();
            dispatchPublic(actions, publicationGate);
        } finally {
            actions.releaseFinalizationHandoffs();
            try {
                proofObservations.finalizePending();
            } finally {
                publicationGate.open();
            }
        }
    }

    private <T> T authoritativeOperationLocked(
        Supplier<T> operation,
        AfterTransition afterTransition
    ) {
        return events.proofFactBatch(
            Objects.requireNonNull(operation, "operation must not be null"),
            Objects.requireNonNull(
                afterTransition,
                "afterTransition must not be null"
            )::addFinalizationHandoff
        );
    }

    record PreparedControlCancellation(
        AfterTransition actions,
        List<Throwable> failures
    ) {
        PreparedControlCancellation {
            actions = Objects.requireNonNull(actions, "actions must not be null");
            failures = List.copyOf(Objects.requireNonNull(
                failures,
                "failures must not be null"
            ));
        }

        private void rethrowFirstFailure() {
            if (failures.isEmpty()) {
                return;
            }
            Throwable failure = failures.getFirst();
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw (Error) failure;
        }
    }

    CompletionGate newCompletionGate() {
        return new CompletionGate();
    }

    void runPreparedInternalActions(PreparedControlCancellation cancellation) {
        Objects.requireNonNull(cancellation, "cancellation must not be null")
            .actions()
            .runInternal();
    }

    void submitPreparedPublicCompletions(
        PreparedControlCancellation cancellation,
        CompletionGate publicationGate
    ) {
        cancellation = Objects.requireNonNull(cancellation, "cancellation must not be null");
        dispatchPublic(cancellation.actions(), publicationGate);
    }

    private void dispatchPublic(AfterTransition actions, CompletionGate publicationGate) {
        for (Runnable completion : actions.publicCompletions()) {
            try {
                completionDispatcher.dispatch(completion, publicationGate);
            } catch (RuntimeException | Error ignored) {
                // Completion delivery infrastructure must never replace framework state.
            }
        }
    }

    private void closeCompletionDispatcherSafely() {
        try {
            completionDispatcher.close();
        } catch (RuntimeException | Error ignored) {
            // Completion delivery infrastructure must never fail environment cleanup.
        }
    }

    static final class CompletionGate {
        private final CountDownLatch published = new CountDownLatch(1);

        void open() {
            published.countDown();
        }

        void awaitPublication() {
            boolean interrupted = false;
            while (true) {
                try {
                    published.await();
                    break;
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    interface CompletionDispatcher extends AutoCloseable {
        void dispatch(Runnable completion, CompletionGate publicationGate);

        @Override
        void close();
    }

    private static final class SystemCompletionDispatcher implements CompletionDispatcher {
        private final Object owner = new Object();
        private final java.util.concurrent.ThreadFactory threadFactory = Thread.ofVirtual()
            .name("system-proof-public-completion-", 0L)
            .factory();
        private int accepted;

        @Override
        public void dispatch(Runnable completion, CompletionGate publicationGate) {
            completion = Objects.requireNonNull(completion, "completion must not be null");
            publicationGate = Objects.requireNonNull(
                publicationGate,
                "publicationGate must not be null"
            );
            Thread thread;
            synchronized (owner) {
                if (accepted >= MAXIMUM_PUBLIC_COMPLETIONS) {
                    throw new IllegalStateException(
                        "Completion dispatcher capacity is exhausted"
                    );
                }
                Runnable acceptedCompletion = completion;
                CompletionGate acceptedGate = publicationGate;
                thread = threadFactory.newThread(() -> {
                    acceptedGate.awaitPublication();
                    try {
                        acceptedCompletion.run();
                    } catch (RuntimeException | Error ignored) {
                        // Public dependents cannot affect framework execution.
                    }
                });
                accepted++;
                try {
                    thread.start();
                } catch (RuntimeException | Error failure) {
                    accepted--;
                    throw failure;
                }
            }
        }

        @Override
        public void close() {
            // No persistent worker or queue exists. In-flight permit outcomes may still
            // publish their already bounded roots after environment teardown.
        }
    }

    private static final class AfterTransition {
        private final List<Runnable> internalActions = new ArrayList<>();
        private final List<Runnable> publicCompletions = new ArrayList<>();
        private final List<ProofFactObserver.FinalizationHandoff> finalizationHandoffs =
            new ArrayList<>();

        private void addInternal(Runnable action) {
            internalActions.add(Objects.requireNonNull(action, "action must not be null"));
        }

        private void addPublic(Runnable completion) {
            publicCompletions.add(Objects.requireNonNull(
                completion,
                "completion must not be null"
            ));
        }

        private void addFinalizationHandoff(
            ProofFactObserver.FinalizationHandoff handoff
        ) {
            finalizationHandoffs.add(Objects.requireNonNull(
                handoff,
                "handoff must not be null"
            ));
        }

        private void runInternal() {
            internalActions.forEach(Runnable::run);
        }

        private List<Runnable> publicCompletions() {
            return List.copyOf(publicCompletions);
        }

        private void releaseFinalizationHandoffs() {
            finalizationHandoffs.forEach(
                ProofFactObserver.FinalizationHandoff::release
            );
            finalizationHandoffs.clear();
        }
    }

    interface TimeoutScheduler extends AutoCloseable {
        TimeoutTask schedule(Duration delay, Runnable action);

        @Override
        void close();
    }

    @FunctionalInterface
    interface TimeoutTask {
        void cancel();
    }

    private static final class SystemTimeoutScheduler implements TimeoutScheduler {
        private final ScheduledExecutorService executor =
            Executors.newSingleThreadScheduledExecutor(runnable ->
                Thread.ofPlatform()
                    .daemon(true)
                    .name("system-proof-semantic-control-timeouts")
                    .unstarted(runnable)
            );

        @Override
        public TimeoutTask schedule(Duration delay, Runnable action) {
            ScheduledFuture<?> future = executor.schedule(
                Objects.requireNonNull(action, "action must not be null"),
                delay.toNanos(),
                TimeUnit.NANOSECONDS
            );
            return () -> future.cancel(false);
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }

    private static final class HoldEntry {
        private final RuntimeSemanticHoldRef ref;
        private final Duration maximumHoldDuration;
        private final ConnectionId connectionId;
        private final io.github.jacekkardys.systemproof.observation.FlowDirection direction;
        private final io.github.jacekkardys.systemproof.observation.EvidenceSchemaId evidenceSchema;
        private final Optional<ProofSubjectRef> proofSubject;
        private final CompletableFuture<InteractionRef> reached = new CompletableFuture<>();
        private final CompletableFuture<SemanticHoldState> completion = new CompletableFuture<>();
        private final CompletableFuture<Void> releaseCompletion = new CompletableFuture<>();
        private Predicate<InteractionRef> evidenceWindow = ignored -> true;
        private SemanticInteractionSelector<?> selector;
        private SemanticHoldState state = SemanticHoldState.DECLARED;
        private InteractionRef interactionRef;
        private SelectorSelection selection;
        private boolean reachedEstablished;
        private CoordinatedPermit permit;
        private TimeoutTask timeoutTask;

        private HoldEntry(
            RuntimeSemanticHoldRef ref,
            SemanticInteractionSelector<?> selector,
            Duration maximumHoldDuration
        ) {
            this.ref = Objects.requireNonNull(ref, "ref must not be null");
            this.selector = Objects.requireNonNull(selector, "selector must not be null");
            this.maximumHoldDuration = Objects.requireNonNull(
                maximumHoldDuration,
                "maximumHoldDuration must not be null"
            );
            connectionId = selector.connectionId();
            direction = selector.direction();
            evidenceSchema = selector.evidenceSchema();
            proofSubject = selector.proofSubject();
        }
    }

    private static final class GuardEntry {
        private final RuntimeSemanticPredecessorGuardRef ref;
        private final ProofSubjectRef subject;
        private final SemanticPredecessorBoundary requiredBoundary;
        private final SemanticInteractionSelector<?> predecessorSelector;
        private final SemanticInteractionSelector<?> successorSelector;
        private final Duration maximumDuration;
        private final CompletableFuture<SemanticPredecessorGuardState> completion =
            new CompletableFuture<>();
        private Predicate<InteractionRef> evidenceWindow = ignored -> true;
        private SemanticPredecessorGuardState state =
            SemanticPredecessorGuardState.DECLARED;
        private InteractionRef predecessor;
        private InteractionRef successor;
        private SelectorSelection predecessorSelection;
        private SelectorSelection successorSelection;
        private TimeoutTask timeoutTask;
        private boolean retainCancelledEnforcement;

        private GuardEntry(
            RuntimeSemanticPredecessorGuardRef ref,
            SemanticPredecessorGuardSpec specification
        ) {
            this.ref = Objects.requireNonNull(ref, "ref must not be null");
            subject = specification.subject();
            requiredBoundary = specification.predecessor().boundary();
            predecessorSelector = specification.predecessor().selector();
            successorSelector = specification.successor();
            maximumDuration = specification.maximumDuration();
        }

        private boolean concerns(ConnectionId connectionId) {
            return predecessorSelector.connectionId().equals(connectionId)
                || successorSelector.connectionId().equals(connectionId);
        }

    }

    record HoldDeclaration(
        SemanticHoldRef ref,
        SemanticHoldState state,
        ConnectionId connectionId,
        Optional<ProofSubjectRef> proofSubject
    ) {
        HoldDeclaration {
            Objects.requireNonNull(ref, "ref must not be null");
            Objects.requireNonNull(state, "state must not be null");
            Objects.requireNonNull(connectionId, "connectionId must not be null");
            Objects.requireNonNull(proofSubject, "proofSubject must not be null");
        }
    }

    record GuardDeclaration(
        SemanticPredecessorGuardRef ref,
        SemanticPredecessorGuardState state,
        ProofSubjectRef subject,
        ConnectionId predecessorConnectionId,
        ConnectionId successorConnectionId
    ) {
        GuardDeclaration {
            Objects.requireNonNull(ref, "ref must not be null");
            Objects.requireNonNull(state, "state must not be null");
            Objects.requireNonNull(subject, "subject must not be null");
            Objects.requireNonNull(
                predecessorConnectionId,
                "predecessorConnectionId must not be null"
            );
            Objects.requireNonNull(
                successorConnectionId,
                "successorConnectionId must not be null"
            );
        }
    }

    private record SelectorSelection(
        SemanticInteractionSelector<?> selector,
        InteractionRef interaction,
        NativeFlowResolution nativeFlow
    ) {
        private SelectorSelection {
            Objects.requireNonNull(selector, "selector must not be null");
            Objects.requireNonNull(interaction, "interaction must not be null");
        }

        private boolean remainsValid(ProofSubjectRegistry proofSubjects) {
            if (nativeFlow != null) {
                return proofSubjects.remainsSoleUniqueNativeFlow(nativeFlow);
            }
            return selector.proofSubject()
                .map(subject -> proofSubjects.isSoleUniqueSubjectFor(subject, interaction))
                .orElse(true);
        }
    }

    private record HoldSelection(HoldEntry entry, SelectorSelection selection) {}

    private record GuardInteractionSelection(
        GuardEntry entry,
        SemanticPredecessorGuardState stateBeforeInteraction,
        SelectorSelection predecessor,
        boolean predecessorFailed,
        SelectorSelection successor,
        boolean successorFailed
    ) {
        private boolean closesSession(InteractionRef interaction) {
            if (predecessorFailed || successorFailed) {
                return true;
            }
            if (successor == null) {
                return false;
            }
            return switch (stateBeforeInteraction) {
                case PREDECESSOR_SATISFIED -> interaction.equals(entry.predecessor);
                case SUCCESSOR_AUTHORIZED, SATISFIED, DECLARED -> false;
                case ARMED, PREDECESSOR_OBSERVED, VIOLATED, CANCELLED, TIMED_OUT, FAILED ->
                    true;
            };
        }
    }

    private record GuardSelections(
        List<GuardInteractionSelection> selections,
        boolean closesSession
    ) {}

    private record HoldMatch(
        HoldEntry entry,
        SelectorSelection selection,
        boolean failedClosed,
        List<HoldEntry> failedEntries,
        SemanticHoldFailure failure
    ) {
        private static HoldMatch none() {
            return new HoldMatch(null, null, false, List.of(), null);
        }

        private static HoldMatch failed(
            List<HoldEntry> entries,
            SemanticHoldFailure failure
        ) {
            return new HoldMatch(null, null, true, List.copyOf(entries), failure);
        }
    }

    private record GuardUse(GuardEntry entry, SelectorSelection selection) {}

    private record GuardDecision(
        boolean closeSession,
        List<GuardUse> forwardedPredecessors,
        List<GuardUse> authorizedSuccessors
    ) {}

    private static final class PermitContext {
        private final HoldEntry hold;
        private final List<GuardUse> forwardedPredecessors;
        private final List<GuardUse> authorizedSuccessors;
        private CoordinatedPermit permit;
        private boolean outcomeClaimed;

        private PermitContext(
            HoldEntry hold,
            List<GuardUse> forwardedPredecessors,
            List<GuardUse> authorizedSuccessors
        ) {
            this.hold = hold;
            this.forwardedPredecessors = forwardedPredecessors;
            this.authorizedSuccessors = authorizedSuccessors;
        }

        private boolean claimOutcome() {
            if (outcomeClaimed) {
                return false;
            }
            outcomeClaimed = true;
            return true;
        }
    }

    private static final class SemanticHoldHandle implements SemanticHold {
        private final SemanticControlCoordinator coordinator;
        private final HoldEntry entry;

        private SemanticHoldHandle(
            SemanticControlCoordinator coordinator,
            HoldEntry entry
        ) {
            this.coordinator = Objects.requireNonNull(
                coordinator,
                "coordinator must not be null"
            );
            this.entry = Objects.requireNonNull(entry, "entry must not be null");
        }

        @Override
        public SemanticHoldRef ref() {
            return entry.ref;
        }

        @Override
        public SemanticHoldState state() {
            return coordinator.state(entry);
        }

        @Override
        public CompletionStage<InteractionRef> reached() {
            return entry.reached.minimalCompletionStage();
        }

        @Override
        public CompletionStage<SemanticHoldState> completion() {
            return entry.completion.minimalCompletionStage();
        }

        @Override
        public CompletionStage<Void> release() {
            return coordinator.release(entry);
        }

        @Override
        public boolean cancel() {
            return coordinator.cancel(entry);
        }
    }

    private static final class SemanticPredecessorGuardHandle
        implements SemanticPredecessorGuard {
        private final SemanticControlCoordinator coordinator;
        private final GuardEntry entry;

        private SemanticPredecessorGuardHandle(
            SemanticControlCoordinator coordinator,
            GuardEntry entry
        ) {
            this.coordinator = Objects.requireNonNull(
                coordinator,
                "coordinator must not be null"
            );
            this.entry = Objects.requireNonNull(entry, "entry must not be null");
        }

        @Override
        public SemanticPredecessorGuardRef ref() {
            return entry.ref;
        }

        @Override
        public SemanticPredecessorGuardState state() {
            return coordinator.state(entry);
        }

        @Override
        public CompletionStage<SemanticPredecessorGuardState> completion() {
            return entry.completion.minimalCompletionStage();
        }

        @Override
        public boolean cancel() {
            return coordinator.cancel(entry);
        }
    }

    private static final class CoordinatedPermit implements ForwardingPermit {
        private final SemanticControlCoordinator coordinator;
        private final PermitContext context;
        private final CompletableFuture<ForwardingDecision> decision =
            new CompletableFuture<>();

        private CoordinatedPermit(
            SemanticControlCoordinator coordinator,
            PermitContext context
        ) {
            this.coordinator = Objects.requireNonNull(
                coordinator,
                "coordinator must not be null"
            );
            this.context = Objects.requireNonNull(context, "context must not be null");
        }

        private void authorize(ForwardingDecision authorization) {
            decision.complete(authorization);
        }

        @Override
        public ForwardingDecision awaitDecision() throws InterruptedException {
            try {
                return decision.get();
            } catch (ExecutionException impossible) {
                throw new CompletionException(impossible.getCause());
            }
        }

        @Override
        public void forwarded() {
            coordinator.forwarded(context);
        }

        @Override
        public void writeFailed() {
            coordinator.writeFailed(context);
        }

        @Override
        public void abandoned() {
            coordinator.abandoned(context);
        }
    }

    private record RuntimeSemanticHoldRef(Object owner, long value)
        implements SemanticHoldRef {
        private RuntimeSemanticHoldRef {
            Objects.requireNonNull(owner, "owner must not be null");
            if (value < FIRST_CONTROL_VALUE) {
                throw new IllegalArgumentException(
                    "semantic-hold value must be at least " + FIRST_CONTROL_VALUE
                );
            }
        }

        @Override
        public String toString() {
            return "semantic-hold-" + value;
        }
    }

    private record RuntimeSemanticPredecessorGuardRef(Object owner, long value)
        implements SemanticPredecessorGuardRef {
        private RuntimeSemanticPredecessorGuardRef {
            Objects.requireNonNull(owner, "owner must not be null");
            if (value < FIRST_CONTROL_VALUE) {
                throw new IllegalArgumentException(
                    "semantic-predecessor-guard value must be at least "
                        + FIRST_CONTROL_VALUE
                );
            }
        }

        @Override
        public String toString() {
            return "semantic-predecessor-guard-" + value;
        }
    }

    private record TerminalPermit(ForwardingDecision decision)
        implements ForwardingPermit {
        private TerminalPermit {
            Objects.requireNonNull(decision, "decision must not be null");
        }

        @Override
        public ForwardingDecision awaitDecision() {
            return decision;
        }

        @Override
        public void forwarded() {}

        @Override
        public void writeFailed() {}

        @Override
        public void abandoned() {}
    }
}
